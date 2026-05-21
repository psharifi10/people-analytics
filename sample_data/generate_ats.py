"""Synthetic ATS (Ashby-shaped) recruiting data generator.

Produces JSON payloads that simulate what an Ashby/Greenhouse-style ATS API
would return. Conforms tightly to the HRIS output: for every post-bootstrap
HRIS hire event, this generator produces the upstream funnel (a winning
application, an accepted offer, and a realistic field of rejected
applications) that led to that hire.

Why conform to HRIS?
--------------------
A real People Analytics warehouse has to answer "of the candidates we
interviewed, what fraction got hired and are still here?". That requires the
ATS data to literally link to the HRIS person who eventually started.
Generating the two stacks independently and praying for a name-match later
is brittle. So this generator *reads* the HRIS outputs and drives off them.

Pipeline:
1. Read ``workday_persons.json`` + ``workday_workers.json`` +
   ``workday_employment_events.json`` from the same output dir.
2. Group hire events into ``Requisition`` objects keyed by
   (function, department, hire_date_window). Multiple hires for the same
   role within a 90-day window collapse into the same requisition.
3. For each requisition, generate:
   - A winning application from the hired candidate (linked to the HRIS
     ``person_external_id``).
   - 10-25 rejected applications from other candidates, with realistic
     stage drop-off.
   - One accepted ``Offer`` (rare: 5% declined, 2% rescinded, 2% expired).
4. Generate 10-15 ``unfilled`` requisitions that received applications but
   were closed without a hire.

Run:
    uv run pa-generate-ats
    uv run pa-generate-ats --seed 43 --output-dir sample_data/generated
"""

from __future__ import annotations

import json
import random
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import click
from faker import Faker

DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "generated"

# Standard recruiting funnel. Order matters - stages must be listed in
# chronological order. Used to walk an application forward stage-by-stage.
STAGES = [
    "applied",
    "recruiter_screen",
    "hiring_manager_screen",
    "technical_interview",
    "final_round",
    "offer",
    "hired",
]

# Probability of being rejected AT each stage (i.e. dropping out before
# advancing). Tune these to shift the funnel shape. Compounding these gives
# the overall offer-rate. Defaults yield ~5% applied -> hired conversion.
STAGE_DROPOFF_PROB = {
    "applied": 0.55,
    "recruiter_screen": 0.45,
    "hiring_manager_screen": 0.40,
    "technical_interview": 0.35,
    "final_round": 0.25,
    "offer": 0.10,
}

REJECTION_REASONS = [
    "did_not_meet_bar",
    "experience_mismatch",
    "compensation_mismatch",
    "location_mismatch",
    "withdrew",
    "did_not_respond",
    "another_candidate_selected",
]

# Source-of-hire mix. Weights sum to ~1.0. LinkedIn dominates inbound for
# tech companies; referrals are the highest-quality source in real funnels.
CANDIDATE_SOURCES = [
    "linkedin",
    "referral",
    "job_board",
    "inbound_application",
    "outbound_sourced",
    "career_site",
    "event",
]
CANDIDATE_SOURCE_WEIGHTS = [0.30, 0.18, 0.15, 0.15, 0.12, 0.08, 0.02]

# Outcomes for an offer that was actually extended to the candidate the
# company wanted. Most accept; a few decline / expire / get rescinded.
OFFER_OUTCOMES = ["accepted", "declined", "expired", "rescinded"]
OFFER_OUTCOME_WEIGHTS_WINNING = [0.91, 0.05, 0.02, 0.02]


@dataclass
class HRISHire:
    worker_id: str
    person_external_id: str
    first_name: str
    last_name: str
    personal_email: str
    hire_date: date
    department: str
    function: str
    job_title: str
    job_level: str


@dataclass
class Requisition:
    requisition_id: str
    job_title: str
    job_level: str
    department: str
    function: str
    opened_at: date
    closed_at: date | None
    status: str  # open / filled / closed_unfilled
    hired_worker_ids: list[str] = field(default_factory=list)


@dataclass
class Candidate:
    candidate_id: str
    first_name: str
    last_name: str
    email: str
    linkedin_url: str
    source: str
    person_external_id: str | None = None


@dataclass
class StageTransition:
    from_stage: str | None
    to_stage: str
    transitioned_at: datetime


@dataclass
class Application:
    application_id: str
    candidate_id: str
    requisition_id: str
    submitted_at: datetime
    current_stage: str
    status: str  # active / rejected / withdrawn / hired
    final_outcome_reason: str | None
    source: str
    transitions: list[StageTransition] = field(default_factory=list)
    is_winning: bool = False
    closed_at: datetime | None = None


@dataclass
class Offer:
    offer_id: str
    application_id: str
    candidate_id: str
    requisition_id: str
    extended_at: datetime
    responded_at: datetime | None
    status: str  # accepted / declined / expired / rescinded
    base_salary_amount: float
    base_salary_currency: str
    accepted_person_external_id: str | None = None


class AtsGenerator:
    def __init__(
        self,
        hris_output_dir: Path,
        output_dir: Path,
        seed: int,
        as_of: date | None = None,
    ) -> None:
        self.hris_output_dir = hris_output_dir
        self.output_dir = output_dir
        self.seed = seed
        self.rng = random.Random(seed)
        self.faker = Faker("en_US")
        Faker.seed(seed)
        self.today = as_of or date.today()

        self._next_req_seq = 1
        self._next_cand_seq = 1
        self._next_app_seq = 1
        self._next_offer_seq = 1

        self.requisitions: list[Requisition] = []
        self.candidates: list[Candidate] = []
        self.applications: list[Application] = []
        self.offers: list[Offer] = []

    # ---------------------------------------------------------------- helpers

    def _new_id(self, prefix: str, attr: str) -> str:
        seq = getattr(self, attr)
        setattr(self, attr, seq + 1)
        return f"{prefix}{seq:07d}"

    def _new_req_id(self) -> str:
        return self._new_id("REQ", "_next_req_seq")

    def _new_cand_id(self) -> str:
        return self._new_id("C", "_next_cand_seq")

    def _new_app_id(self) -> str:
        return self._new_id("APP", "_next_app_seq")

    def _new_offer_id(self) -> str:
        return self._new_id("OFR", "_next_offer_seq")

    def _pick_source(self) -> str:
        return self.rng.choices(CANDIDATE_SOURCES, weights=CANDIDATE_SOURCE_WEIGHTS, k=1)[0]

    def _utc(self, d: date, offset_days: int = 0, offset_hours: int = 0) -> datetime:
        dt = datetime.combine(d, datetime.min.time(), tzinfo=timezone.utc)
        return dt + timedelta(days=offset_days, hours=offset_hours)

    # ------------------------------------------------------------------ load

    def _load_hris_hires(self) -> list[HRISHire]:
        """Read the three HRIS payloads and emit one HRISHire per real hire.

        Skips the bootstrap mass-hire (the day in HRIS where ~1000 employees
        are seeded at the start of history) because those people would not
        have a real funnel - they pre-date the company.
        """
        persons_path = self.hris_output_dir / "workday_persons.json"
        workers_path = self.hris_output_dir / "workday_workers.json"
        events_path = self.hris_output_dir / "workday_employment_events.json"

        for p in (persons_path, workers_path, events_path):
            if not p.exists():
                raise click.ClickException(
                    f"HRIS payload not found at {p}. Run pa-generate-data first."
                )

        # Index persons + workers by their natural keys for O(1) lookup
        # while we walk the events stream.
        persons = {p["person_external_id"]: p for p in json.loads(persons_path.read_text())}
        workers = {w["worker_id"]: w for w in json.loads(workers_path.read_text())}
        events = json.loads(events_path.read_text())

        # Detect the bootstrap day: any single date with >= 50 hire events
        # is almost certainly the seeded initial population, not real flow.
        date_counts: dict[str, int] = defaultdict(int)
        for ev in events:
            if ev["event_type"] in {"hire", "rehire"}:
                date_counts[ev["effective_date"]] += 1
        bootstrap_date: str | None = None
        if date_counts:
            top_date, top_count = max(date_counts.items(), key=lambda kv: kv[1])
            if top_count >= 50:
                bootstrap_date = top_date

        hires: list[HRISHire] = []
        for ev in events:
            if ev["event_type"] not in {"hire", "rehire"}:
                continue
            if bootstrap_date and ev["effective_date"] == bootstrap_date:
                continue
            worker = workers.get(ev["worker_id"])
            person = persons.get(ev["person_external_id"])
            if worker is None or person is None:
                continue
            hires.append(
                HRISHire(
                    worker_id=ev["worker_id"],
                    person_external_id=ev["person_external_id"],
                    first_name=person["first_name"],
                    last_name=person["last_name"],
                    personal_email=person["personal_email"],
                    hire_date=date.fromisoformat(ev["effective_date"]),
                    department=ev["department"],
                    function=ev["function"],
                    job_title=ev["job_title"],
                    job_level=ev["job_level"],
                )
            )
        hires.sort(key=lambda h: h.hire_date)
        return hires

    # ------------------------------------------------------------------ main

    def run(self) -> None:
        hires = self._load_hris_hires()
        self._build_filled_requisitions(hires)
        self._build_unfilled_requisitions()
        self._write_outputs()

    def _build_filled_requisitions(self, hires: list[HRISHire]) -> None:
        """Group HRIS hires into requisitions and synthesise the funnel.

        Multiple hires for the same role within a 90-day window collapse
        into a single requisition (e.g. a "hire 3 platform engineers" req).
        Each grouping gets opened ~35-110 days before the first hire and
        closes on the last hire date.
        """
        # Bucket hires that share function + department + title + 90-day window
        groups: dict[tuple[str, str, str, str], list[HRISHire]] = defaultdict(list)
        for h in hires:
            key = (h.function, h.department, h.job_title, self._req_window(h.hire_date))
            groups[key].append(h)

        for (function, department, title, _window), members in groups.items():
            first_hire = min(members, key=lambda h: h.hire_date)
            last_hire = max(members, key=lambda h: h.hire_date)
            # Requisitions open weeks before the first hire (recruitment cycle)
            opened_at = first_hire.hire_date - timedelta(days=self.rng.randint(35, 110))
            closed_at = last_hire.hire_date
            req = Requisition(
                requisition_id=self._new_req_id(),
                job_title=title,
                job_level=members[0].job_level,
                department=department,
                function=function,
                opened_at=opened_at,
                closed_at=closed_at,
                status="filled",
                hired_worker_ids=[h.worker_id for h in members],
            )
            self.requisitions.append(req)

            # The "winning" application(s) - one per HRIS hire in this group
            for h in members:
                self._make_winning_application(req, h)

            # Plus a realistic field of rejected applications. 10-25 per slot
            # produces a ~4-7% offer rate which matches industry benchmarks.
            n_rejected = self.rng.randint(10, 25) * len(members)
            for _ in range(n_rejected):
                self._make_rejected_application(req)

    def _req_window(self, hire_date: date) -> str:
        # Bucket dates into 90-day chunks via ordinal arithmetic. Two hires
        # in the same bucket + same role share a requisition.
        bucket = (hire_date.toordinal() // 90) * 90
        return str(bucket)

    def _build_unfilled_requisitions(self) -> None:
        """Add reqs that received applicants but did NOT result in a hire.

        Two flavours:
        - "open": still live, has active applicants in the funnel.
        - "closed_unfilled": closed without filling (priorities changed,
          hiring freeze, etc). Realistic in any growth-stage company.
        """
        n = self.rng.randint(10, 18)
        # Reuse role definitions from the filled reqs so titles look real
        functions = list({(r.function, r.department, r.job_title, r.job_level) for r in self.requisitions})
        if not functions:
            return
        for _ in range(n):
            function, department, title, level = self.rng.choice(functions)
            span_days = self.rng.randint(60, 180)
            opened_at = self.today - timedelta(days=self.rng.randint(span_days + 30, span_days + 365))
            if self.rng.random() < 0.4:
                # 40% chance the req is still open and gathering applicants
                status = "open"
                closed_at = None
            else:
                status = "closed_unfilled"
                closed_at = opened_at + timedelta(days=span_days)
            req = Requisition(
                requisition_id=self._new_req_id(),
                job_title=title,
                job_level=level,
                department=department,
                function=function,
                opened_at=opened_at,
                closed_at=closed_at,
                status=status,
            )
            self.requisitions.append(req)

            n_apps = self.rng.randint(8, 20)
            for _ in range(n_apps):
                # force_active=True so open reqs keep candidates in flight
                # rather than marking them rejected
                self._make_rejected_application(req, force_active=(status == "open"))

    # ------------------------------------------------------------- applications

    def _make_candidate(self, hire: HRISHire | None = None) -> Candidate:
        """Create a candidate. If `hire` is supplied, this candidate is the
        "winning" applicant who was actually hired - reuse their HRIS person
        identity so the offer can join back to dim_employee downstream."""
        if hire is not None:
            # Winning candidate: real person, already in HRIS
            first, last = hire.first_name, hire.last_name
            email = hire.personal_email
            person_external_id = hire.person_external_id
        else:
            # Random rejected applicant: synthetic identity, no HRIS link
            first = self.faker.first_name()
            last = self.faker.last_name()
            email = f"{first.lower()}.{last.lower()}.{self.rng.randint(1000, 9999)}@example.org"
            person_external_id = None
        cand = Candidate(
            candidate_id=self._new_cand_id(),
            first_name=first,
            last_name=last,
            email=email,
            linkedin_url=f"https://www.linkedin.com/in/{first.lower()}-{last.lower()}-{self.rng.randint(1000, 9999)}",
            source=self._pick_source(),
            person_external_id=person_external_id,
        )
        self.candidates.append(cand)
        return cand

    def _make_winning_application(self, req: Requisition, hire: HRISHire) -> None:
        """Build the application that resulted in the HRIS hire.

        Walks the candidate through every stage in order, threads timestamps
        forward, then applies a small chance the offer was declined
        (in which case the application is marked rejected even though the
        funnel ran to "offer").
        """
        cand = self._make_candidate(hire=hire)
        # Submitted somewhere between req opening and 30 days before hire
        # (leaves room for the funnel to run)
        submitted_at = self._utc(
            req.opened_at + timedelta(days=self.rng.randint(0, max(1, (hire.hire_date - req.opened_at).days - 30)))
        )

        # Walk through every stage with realistic gaps between transitions
        transitions: list[StageTransition] = []
        prev_stage: str | None = None
        cur_at = submitted_at
        for stage in STAGES:
            transitions.append(StageTransition(from_stage=prev_stage, to_stage=stage, transitioned_at=cur_at))
            prev_stage = stage
            cur_at += timedelta(days=self.rng.randint(2, 9), hours=self.rng.randint(0, 23))

        offer_transition = next(t for t in transitions if t.to_stage == "offer")
        hired_transition = next(t for t in transitions if t.to_stage == "hired")
        # Anchor the "hired" transition exactly on the HRIS hire_date so the
        # two systems agree on when the person actually started.
        hired_transition.transitioned_at = self._utc(hire.hire_date)

        # Decide if the offer was actually accepted (most are). If declined /
        # expired / rescinded, drop the "hired" stage from the history.
        outcome = self.rng.choices(OFFER_OUTCOMES, weights=OFFER_OUTCOME_WEIGHTS_WINNING, k=1)[0]
        if outcome != "accepted":
            transitions = [t for t in transitions if t.to_stage != "hired"]
            status = "rejected"
            final_reason = "offer_" + outcome
        else:
            status = "hired"
            final_reason = None

        app = Application(
            application_id=self._new_app_id(),
            candidate_id=cand.candidate_id,
            requisition_id=req.requisition_id,
            submitted_at=submitted_at,
            current_stage=transitions[-1].to_stage,
            status=status,
            final_outcome_reason=final_reason,
            source=cand.source,
            transitions=transitions,
            is_winning=(outcome == "accepted"),
            closed_at=transitions[-1].transitioned_at,
        )
        self.applications.append(app)

        # Always emit an offer record for the winning application, even when
        # it was declined. Recruiting analytics needs offer-accept rates.
        offer = Offer(
            offer_id=self._new_offer_id(),
            application_id=app.application_id,
            candidate_id=cand.candidate_id,
            requisition_id=req.requisition_id,
            extended_at=offer_transition.transitioned_at,
            responded_at=transitions[-1].transitioned_at,
            status=outcome,
            base_salary_amount=self._salary_for(req.job_level),
            base_salary_currency="CAD",
            accepted_person_external_id=hire.person_external_id if outcome == "accepted" else None,
        )
        self.offers.append(offer)

    def _make_rejected_application(self, req: Requisition, force_active: bool = False) -> None:
        """Synthesise an application that does NOT result in a hire.

        Walks the candidate forward stage-by-stage, dropping out at each
        stage with the configured probability (STAGE_DROPOFF_PROB). Once a
        drop-out roll succeeds the loop breaks and the rest of the stages
        never appear.

        force_active=True suppresses the dropout roll AND the rejection
        outcome - used for live applicants in still-open requisitions.
        """
        cand = self._make_candidate()
        # For closed reqs, applications can have arrived any time during the
        # req's lifetime. For open reqs, anytime up to today.
        if req.closed_at is None:
            window_end = self.today
        else:
            window_end = req.closed_at
        days_range = max(1, (window_end - req.opened_at).days)
        submitted_at = self._utc(req.opened_at + timedelta(days=self.rng.randint(0, days_range)))

        # Walk forward; break out of the loop the moment we drop out
        transitions: list[StageTransition] = []
        prev_stage: str | None = None
        cur_at = submitted_at
        for stage in STAGES[:-1]:  # never reach "hired" for rejected
            transitions.append(StageTransition(from_stage=prev_stage, to_stage=stage, transitioned_at=cur_at))
            prev_stage = stage
            # Standard funnel drop-out
            if self.rng.random() < STAGE_DROPOFF_PROB[stage] and not force_active:
                break
            # For open reqs we still want SOME variation in current_stage
            if force_active and self.rng.random() < 0.3:
                break
            cur_at += timedelta(days=self.rng.randint(2, 12), hours=self.rng.randint(0, 23))

        if force_active:
            status = "active"
            final_reason = None
            closed_at = None
        else:
            # 10% of failures are candidate-driven withdrawals, rest are
            # company-driven rejections
            status = "rejected" if self.rng.random() > 0.1 else "withdrawn"
            final_reason = self.rng.choice(REJECTION_REASONS)
            closed_at = cur_at + timedelta(days=self.rng.randint(1, 5))

        app = Application(
            application_id=self._new_app_id(),
            candidate_id=cand.candidate_id,
            requisition_id=req.requisition_id,
            submitted_at=submitted_at,
            current_stage=transitions[-1].to_stage,
            status=status,
            final_outcome_reason=final_reason,
            source=cand.source,
            transitions=transitions,
            is_winning=False,
            closed_at=closed_at,
        )
        self.applications.append(app)

    def _salary_for(self, job_level: str) -> float:
        base = {
            "IC1": 70000, "IC2": 90000, "IC3": 115000, "IC4": 140000, "IC5": 170000,
            "M1": 145000, "M2": 175000, "M3": 215000, "M4": 280000,
        }.get(job_level, 100000)
        return round(base * self.rng.uniform(0.92, 1.10), -2)

    # -------------------------------------------------------------- write out

    def _write_outputs(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        extract_ts = datetime.now(timezone.utc).isoformat()

        jobs_payload = [
            {
                "requisition_id": r.requisition_id,
                "job_title": r.job_title,
                "job_level": r.job_level,
                "department": r.department,
                "function": r.function,
                "opened_at": r.opened_at.isoformat(),
                "closed_at": r.closed_at.isoformat() if r.closed_at else None,
                "status": r.status,
                "hired_worker_ids": r.hired_worker_ids,
                "source_updated_at": extract_ts,
            }
            for r in self.requisitions
        ]

        candidates_payload = [
            {
                "candidate_id": c.candidate_id,
                "first_name": c.first_name,
                "last_name": c.last_name,
                "email": c.email,
                "linkedin_url": c.linkedin_url,
                "source": c.source,
                "person_external_id": c.person_external_id,
                "source_updated_at": extract_ts,
            }
            for c in self.candidates
        ]

        applications_payload = [
            {
                "application_id": a.application_id,
                "candidate_id": a.candidate_id,
                "requisition_id": a.requisition_id,
                "submitted_at": a.submitted_at.isoformat(),
                "current_stage": a.current_stage,
                "status": a.status,
                "final_outcome_reason": a.final_outcome_reason,
                "source": a.source,
                "is_winning": a.is_winning,
                "closed_at": a.closed_at.isoformat() if a.closed_at else None,
                "stage_history": [
                    {
                        "from_stage": t.from_stage,
                        "to_stage": t.to_stage,
                        "transitioned_at": t.transitioned_at.isoformat(),
                    }
                    for t in a.transitions
                ],
                "source_updated_at": extract_ts,
            }
            for a in self.applications
        ]

        offers_payload = [
            {
                "offer_id": o.offer_id,
                "application_id": o.application_id,
                "candidate_id": o.candidate_id,
                "requisition_id": o.requisition_id,
                "extended_at": o.extended_at.isoformat(),
                "responded_at": o.responded_at.isoformat() if o.responded_at else None,
                "status": o.status,
                "base_salary_amount": o.base_salary_amount,
                "base_salary_currency": o.base_salary_currency,
                "accepted_person_external_id": o.accepted_person_external_id,
                "source_updated_at": extract_ts,
            }
            for o in self.offers
        ]

        manifest = {
            "generated_at_utc": extract_ts,
            "seed": self.seed,
            "as_of_date": self.today.isoformat(),
            "requisitions": len(self.requisitions),
            "requisitions_filled": sum(1 for r in self.requisitions if r.status == "filled"),
            "requisitions_unfilled": sum(1 for r in self.requisitions if r.status == "closed_unfilled"),
            "requisitions_open": sum(1 for r in self.requisitions if r.status == "open"),
            "candidates": len(self.candidates),
            "applications": len(self.applications),
            "applications_hired": sum(1 for a in self.applications if a.status == "hired"),
            "applications_rejected": sum(1 for a in self.applications if a.status == "rejected"),
            "applications_active": sum(1 for a in self.applications if a.status == "active"),
            "offers": len(self.offers),
            "offers_accepted": sum(1 for o in self.offers if o.status == "accepted"),
        }

        for name, payload in (
            ("ashby_jobs.json", jobs_payload),
            ("ashby_candidates.json", candidates_payload),
            ("ashby_applications.json", applications_payload),
            ("ashby_offers.json", offers_payload),
            ("_manifest_ats.json", manifest),
        ):
            (self.output_dir / name).write_text(json.dumps(payload, indent=2, default=str))


@click.command()
@click.option("--seed", default=43, help="Random seed for reproducibility.")
@click.option(
    "--hris-output-dir",
    default=str(DEFAULT_OUTPUT_DIR),
    help="Where to find the HRIS JSON payloads (workday_*.json). Synth ATS data conforms to these.",
)
@click.option(
    "--output-dir",
    default=str(DEFAULT_OUTPUT_DIR),
    help="Where to write ATS JSON payloads.",
)
def main(seed: int, hris_output_dir: str, output_dir: str) -> None:
    """Generate synthetic Ashby-shaped ATS source payloads, conformed to HRIS hires."""
    hris_dir = Path(hris_output_dir)
    out_dir = Path(output_dir)
    gen = AtsGenerator(hris_output_dir=hris_dir, output_dir=out_dir, seed=seed)
    click.echo(f"Generating ATS payloads conformed to HRIS hires at {hris_dir}...")
    gen.run()
    click.echo(f"Wrote ATS payloads to {out_dir}")
    manifest = json.loads((out_dir / "_manifest_ats.json").read_text())
    for k, v in manifest.items():
        click.echo(f"  {k}: {v}")


if __name__ == "__main__":
    main()
