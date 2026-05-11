"""Synthetic People Analytics source data generator.

Produces JSON payloads that simulate what the HRIS (Workday-shaped) and ATS
(Ashby-shaped) APIs would return. The output mirrors source schemas closely
enough that the same downstream extractors and dbt models can later be pointed
at real APIs by swapping the source-mode flag.

Design choices
--------------
* Deterministic: a fixed seed makes runs reproducible so dbt tests are stable.
* Append-history shaped: every employee carries an array of effective-dated
  profile records (the HRIS "worker history"), not just a current snapshot.
  This is what lets us prove SCD2 and point-in-time correctness end-to-end.
* Realistic-enough: we don't try to model salary curves or perf cycles,
  but we do generate hires, terminations, promotions, transfers, and rehires.
* Three years of history, ~1,000 steady-state active employees.

Run:
    uv run pa-generate-data
    # or with overrides
    uv run pa-generate-data --employees 500 --years 2 --seed 7
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

# -- Configuration --------------------------------------------------------------------

DEFAULT_OUTPUT_DIR = Path(__file__).resolve().parent / "generated"

# Functional org tree: function -> list of departments. Drives the synth so
# every employee belongs to a real-looking unit. Keep narrow enough that
# downstream group-by counts are meaningful at sample-data scale.
ORG_TREE: dict[str, list[str]] = {
    "Engineering": ["Platform", "Product Eng", "Data", "Security"],
    "Product & Design": ["Product Mgmt", "Design"],
    "Go-to-Market": ["Sales", "Marketing", "Customer Success"],
    "G&A": ["People", "Finance", "Legal"],
}

# (city, region, country) tuples. Region is None for "Remote" because real
# Workday leaves that field blank for fully-remote workers.
LOCATIONS = [
    ("Toronto", "ON", "CA"),
    ("Vancouver", "BC", "CA"),
    ("Montreal", "QC", "CA"),
    ("New York", "NY", "US"),
    ("Remote", None, "CA"),
]

# Worker-type mix. Heavy full-time so the headcount story is clean.
WORKER_TYPES = ["full_time", "part_time", "contractor", "intern", "contingent"]
WORKER_TYPE_WEIGHTS = [0.85, 0.05, 0.06, 0.02, 0.02]

# IC1-IC5 individual contributor ladder; M1-M4 management ladder.
JOB_LEVELS = ["IC1", "IC2", "IC3", "IC4", "IC5", "M1", "M2", "M3", "M4"]

# Every event_type produced by the synth. The "rehire" event is what links
# a second Employment back to an existing Person (cross-stint identity).
EVENT_TYPES = ["hire", "promotion", "transfer", "lateral", "termination", "rehire"]

# Termination reason mix. Heavy voluntary_resignation matches reality and
# is what makes voluntary-attrition the headline metric.
TERMINATION_REASONS = [
    "voluntary_resignation",
    "voluntary_relocation",
    "involuntary_performance",
    "involuntary_layoff",
    "end_of_contract",
]


# -- Domain objects -------------------------------------------------------------------


@dataclass
class ProfileVersion:
    """One effective-dated row of a worker's profile (drives SCD2 downstream)."""

    effective_date: date
    department: str
    function: str
    job_title: str
    job_level: str
    manager_worker_id: str | None
    location_city: str
    location_region: str | None
    location_country: str
    worker_type: str
    fte: float
    event_type: str  # what change caused this version


@dataclass
class Employment:
    """A contiguous stint at the company. Rehires create a second Employment."""

    worker_id: str  # source HRIS worker id (string id, immutable per stint)
    hire_date: date
    termination_date: date | None
    termination_reason: str | None
    profile_versions: list[ProfileVersion] = field(default_factory=list)


@dataclass
class Person:
    """A real-world person; can have multiple Employments via rehire."""

    person_external_id: str  # stable cross-stint id (rare in real Workday, but realistic for our synth)
    first_name: str
    last_name: str
    work_email_template: str  # we re-derive per-stint
    personal_email: str
    employments: list[Employment] = field(default_factory=list)


# -- Generator ------------------------------------------------------------------------


class Generator:
    def __init__(
        self,
        employees: int,
        years: int,
        seed: int,
        output_dir: Path,
        as_of: date | None = None,
    ) -> None:
        self.target_active = employees
        self.years = years
        self.seed = seed
        self.output_dir = output_dir
        self.rng = random.Random(seed)
        self.faker = Faker("en_US")
        Faker.seed(seed)

        self.today = as_of or date.today()
        self.start_date = date(self.today.year - years, 1, 1)
        self.persons: list[Person] = []
        self._next_person_seq = 1
        self._next_worker_seq = 1
        self._used_emails: set[str] = set()

    # --- helpers ------------------------------------------------------------------

    def _new_person_id(self) -> str:
        pid = f"P{self._next_person_seq:07d}"
        self._next_person_seq += 1
        return pid

    def _new_worker_id(self) -> str:
        wid = f"W{self._next_worker_seq:07d}"
        self._next_worker_seq += 1
        return wid

    def _uniq_work_email(self, first: str, last: str) -> str:
        base = f"{first.lower()}.{last.lower()}".replace(" ", "")
        candidate = f"{base}@example.com"
        i = 2
        while candidate in self._used_emails:
            candidate = f"{base}{i}@example.com"
            i += 1
        self._used_emails.add(candidate)
        return candidate

    def _pick_location(self) -> tuple[str, str | None, str]:
        return self.rng.choice(LOCATIONS)

    def _pick_function_dept(self) -> tuple[str, str]:
        function = self.rng.choices(
            list(ORG_TREE.keys()), weights=[6, 3, 5, 2], k=1
        )[0]
        dept = self.rng.choice(ORG_TREE[function])
        return function, dept

    def _pick_worker_type(self) -> str:
        return self.rng.choices(WORKER_TYPES, weights=WORKER_TYPE_WEIGHTS, k=1)[0]

    def _fte_for(self, worker_type: str) -> float:
        if worker_type == "part_time":
            return self.rng.choice([0.4, 0.5, 0.6, 0.8])
        if worker_type in {"intern", "contingent"}:
            return self.rng.choice([0.5, 1.0])
        return 1.0

    def _pick_job(self, function: str, level_hint: str | None = None) -> tuple[str, str]:
        titles = {
            "Engineering": ["Software Engineer", "Staff Engineer", "Engineering Manager"],
            "Product & Design": ["Product Manager", "Designer", "Senior PM"],
            "Go-to-Market": ["Account Executive", "Marketing Manager", "CSM"],
            "G&A": ["Recruiter", "Accountant", "People Partner"],
        }
        title = self.rng.choice(titles.get(function, ["Specialist"]))
        level = level_hint or self.rng.choice(JOB_LEVELS[:6])
        if "Manager" in title or "Staff" in title:
            level = self.rng.choice(["M1", "M2", "IC5"])
        return title, level

    # --- core simulation ----------------------------------------------------------

    def run(self) -> None:
        self._bootstrap_initial_population()
        self._simulate_history()
        self._write_outputs()

    def _bootstrap_initial_population(self) -> None:
        """Seed the company at start_date with active employees and a manager tree."""
        # 1. Create the CEO first so everyone has an ancestor.
        ceo = self._make_person()
        ceo_emp = self._open_employment(ceo, self.start_date, manager_worker_id=None,
                                        function="G&A", department="People",  # CEO sits in G&A for simplicity
                                        title="CEO", level="M4", event_type="hire")
        managers_pool: list[str] = [ceo_emp.worker_id]

        # 2. Create function leaders.
        function_leads: dict[str, str] = {}
        for fn in ORG_TREE:
            p = self._make_person()
            emp = self._open_employment(p, self.start_date, manager_worker_id=ceo_emp.worker_id,
                                        function=fn, department=ORG_TREE[fn][0],
                                        title=f"VP, {fn}", level="M3", event_type="hire")
            function_leads[fn] = emp.worker_id
            managers_pool.append(emp.worker_id)

        # 3. Create department managers.
        dept_managers: dict[tuple[str, str], str] = {}
        for fn, depts in ORG_TREE.items():
            for d in depts:
                p = self._make_person()
                emp = self._open_employment(p, self.start_date, manager_worker_id=function_leads[fn],
                                            function=fn, department=d,
                                            title=f"Director, {d}", level="M2", event_type="hire")
                dept_managers[(fn, d)] = emp.worker_id
                managers_pool.append(emp.worker_id)

        # 4. Fill ICs up to target headcount.
        existing = sum(len(p.employments) for p in self.persons)
        ic_target = self.target_active - existing
        for _ in range(ic_target):
            p = self._make_person()
            fn, d = self._pick_function_dept()
            mgr = dept_managers[(fn, d)]
            self._open_employment(p, self.start_date, manager_worker_id=mgr,
                                  function=fn, department=d,
                                  title=None, level=None, event_type="hire")

    def _make_person(self) -> Person:
        first = self.faker.first_name()
        last = self.faker.last_name()
        person = Person(
            person_external_id=self._new_person_id(),
            first_name=first,
            last_name=last,
            work_email_template=f"{first.lower()}.{last.lower()}@example.com",
            personal_email=f"{first.lower()}.{last.lower()}.{self.rng.randint(1000,9999)}@gmail.com",
        )
        self.persons.append(person)
        return person

    def _open_employment(
        self,
        person: Person,
        hire_date: date,
        manager_worker_id: str | None,
        function: str,
        department: str,
        title: str | None,
        level: str | None,
        event_type: str,
    ) -> Employment:
        worker_id = self._new_worker_id()
        wt = self._pick_worker_type()
        fte = self._fte_for(wt)
        loc = self._pick_location()
        if title is None or level is None:
            title, level = self._pick_job(function)
        emp = Employment(
            worker_id=worker_id,
            hire_date=hire_date,
            termination_date=None,
            termination_reason=None,
            profile_versions=[
                ProfileVersion(
                    effective_date=hire_date,
                    department=department,
                    function=function,
                    job_title=title,
                    job_level=level,
                    manager_worker_id=manager_worker_id,
                    location_city=loc[0],
                    location_region=loc[1],
                    location_country=loc[2],
                    worker_type=wt,
                    fte=fte,
                    event_type=event_type,
                )
            ],
        )
        person.employments.append(emp)
        return emp

    # --- ongoing events -----------------------------------------------------------

    def _simulate_history(self) -> None:
        """Walk day-by-day applying hires, terminations, promotions, transfers."""
        cur = self.start_date + timedelta(days=1)
        # Annualised rates (realistic ranges for a growth-stage company)
        annual_attrition = 0.15
        annual_promotion = 0.12
        annual_transfer = 0.05
        # Convert annual rates to per-employee-per-day probabilities. With
        # ~1000 active employees this yields ~150 terms / 120 promos / 50
        # transfers per year -- realistic flow volumes.
        p_attrition = annual_attrition / 365
        p_promo = annual_promotion / 365
        p_transfer = annual_transfer / 365

        while cur <= self.today:
            actives = self._active_employments(cur)
            for emp in list(actives):
                # Skip events on the very day of hire (no one promotes on day 1)
                if cur == emp.hire_date:
                    continue
                roll = self.rng.random()
                if roll < p_attrition:
                    self._terminate(emp, cur)
                    continue
                # Independent roll: an employee can promote OR transfer but
                # only one per day.
                roll2 = self.rng.random()
                if roll2 < p_promo:
                    self._promote(emp, cur)
                elif roll2 < p_promo + p_transfer:
                    self._transfer(emp, cur)

            # Backfill hires + growth: aim for ~5% headcount drift per year
            # above attrition. Without this the population drains over time.
            target_today = int(self.target_active * (1 + 0.05 * ((cur - self.start_date).days / 365)))
            current_active = len(self._active_employments(cur))
            deficit = target_today - current_active
            # Spread the deficit across the year -- small chance per day so
            # hires are sprinkled rather than batched.
            if deficit > 0 and self.rng.random() < (deficit / 365):
                self._hire_new(cur)

            # Occasional rehire: someone who terminated 6+ months ago comes back.
            # Low probability -- rehires are rare in real data.
            if self.rng.random() < 0.0008:
                self._maybe_rehire(cur)

            cur += timedelta(days=1)

    def _active_employments(self, on: date) -> list[Employment]:
        out: list[Employment] = []
        for p in self.persons:
            for e in p.employments:
                if e.hire_date <= on and (e.termination_date is None or e.termination_date > on):
                    out.append(e)
        return out

    def _terminate(self, emp: Employment, on: date) -> None:
        # Don't terminate the CEO or VP layer (keeps the org coherent for the demo).
        # Without this guard the synth can produce an org tree with no leaders,
        # which breaks the manager-chain recursive CTE downstream.
        top = emp.profile_versions[-1]
        if top.job_level in {"M3", "M4"}:
            return
        emp.termination_date = on
        # Voluntary terms dominate (weight 6 vs 1 for each other reason)
        emp.termination_reason = self.rng.choices(
            TERMINATION_REASONS, weights=[6, 1, 1, 1, 1], k=1
        )[0]
        # Append a final ProfileVersion with event_type='termination' so the
        # SCD2 chain has a clean closing boundary downstream.
        emp.profile_versions.append(
            ProfileVersion(
                effective_date=on,
                department=top.department,
                function=top.function,
                job_title=top.job_title,
                job_level=top.job_level,
                manager_worker_id=top.manager_worker_id,
                location_city=top.location_city,
                location_region=top.location_region,
                location_country=top.location_country,
                worker_type=top.worker_type,
                fte=top.fte,
                event_type="termination",
            )
        )

    def _promote(self, emp: Employment, on: date) -> None:
        top = emp.profile_versions[-1]
        try:
            idx = JOB_LEVELS.index(top.job_level)
        except ValueError:
            return
        if idx >= len(JOB_LEVELS) - 1:
            return
        new_level = JOB_LEVELS[idx + 1]
        new_title = top.job_title
        if new_level.startswith("M") and not top.job_title.startswith(("Manager", "Director", "VP", "Senior")):
            new_title = f"Senior {top.job_title}"
        emp.profile_versions.append(
            ProfileVersion(
                effective_date=on,
                department=top.department,
                function=top.function,
                job_title=new_title,
                job_level=new_level,
                manager_worker_id=top.manager_worker_id,
                location_city=top.location_city,
                location_region=top.location_region,
                location_country=top.location_country,
                worker_type=top.worker_type,
                fte=top.fte,
                event_type="promotion",
            )
        )

    def _transfer(self, emp: Employment, on: date) -> None:
        top = emp.profile_versions[-1]
        new_fn, new_dept = self._pick_function_dept()
        if (new_fn, new_dept) == (top.function, top.department):
            return
        # Find a manager in the new department on this date
        candidates = [
            e for e in self._active_employments(on)
            if e.profile_versions[-1].department == new_dept
            and e.profile_versions[-1].job_level.startswith("M")
            and e.worker_id != emp.worker_id
        ]
        if not candidates:
            return
        new_mgr = self.rng.choice(candidates).worker_id
        emp.profile_versions.append(
            ProfileVersion(
                effective_date=on,
                department=new_dept,
                function=new_fn,
                job_title=top.job_title,
                job_level=top.job_level,
                manager_worker_id=new_mgr,
                location_city=top.location_city,
                location_region=top.location_region,
                location_country=top.location_country,
                worker_type=top.worker_type,
                fte=top.fte,
                event_type="transfer",
            )
        )

    def _hire_new(self, on: date) -> None:
        person = self._make_person()
        fn, d = self._pick_function_dept()
        # Find a manager in that department
        mgrs = [
            e.worker_id for e in self._active_employments(on)
            if e.profile_versions[-1].department == d
            and e.profile_versions[-1].job_level.startswith("M")
        ]
        mgr = self.rng.choice(mgrs) if mgrs else None
        self._open_employment(person, on, manager_worker_id=mgr, function=fn, department=d,
                              title=None, level=None, event_type="hire")

    def _maybe_rehire(self, on: date) -> None:
        """Bring back a previously-terminated person under a new worker_id.

        This is what creates a Person with multiple Employments -- the
        cross-stint identity that justifies the dim_person vs dim_employee
        split in the warehouse.
        """
        # Pick a person whose only employment ended >180d ago and was voluntary.
        # 180d gap stops "instant rehire" patterns that don't match reality.
        candidates = [
            p for p in self.persons
            if p.employments
            and all(e.termination_date is not None for e in p.employments)
            and (on - max(e.termination_date for e in p.employments if e.termination_date)).days > 180  # type: ignore[type-var]
            and any(e.termination_reason and "voluntary" in e.termination_reason for e in p.employments)
        ]
        if not candidates:
            return
        person = self.rng.choice(candidates)
        fn, d = self._pick_function_dept()
        mgrs = [
            e.worker_id for e in self._active_employments(on)
            if e.profile_versions[-1].department == d
            and e.profile_versions[-1].job_level.startswith("M")
        ]
        mgr = self.rng.choice(mgrs) if mgrs else None
        # event_type='rehire' so the warehouse can distinguish first-time
        # hires from rehires (different cohort metrics).
        self._open_employment(person, on, manager_worker_id=mgr, function=fn, department=d,
                              title=None, level=None, event_type="rehire")

    # --- output -------------------------------------------------------------------

    def _write_outputs(self) -> None:
        """Serialise three Workday-shaped JSON payloads + a manifest.

        These three payloads are what a real Workday RaaS extract would look
        like: a workers feed (one row per stint with embedded history), a
        persons feed (cross-stint identity), and an events feed (the change
        history). The downstream extractor reads all three and lands them
        flat.
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)
        # 1. Workers extract: one record per worker_id (employment stint),
        #    with embedded profile history that the extractor will unfurl
        #    into its own raw table.
        workers = self._build_workers_payload()
        # 2. Persons extract: cross-stint identity; lets us prove rehires
        #    collapse back to one dim_person.
        persons = self._build_persons_payload()
        # 3. Employment events: flat list, drives fact_employment_event.
        events = self._build_events_payload()

        extract_ts = datetime.now(timezone.utc).isoformat()
        meta = {
            "generated_at_utc": extract_ts,
            "seed": self.seed,
            "years_of_history": self.years,
            "persons": len(self.persons),
            "employments": sum(len(p.employments) for p in self.persons),
            "active_today": len(self._active_employments(self.today)),
            "as_of_date": self.today.isoformat(),
        }

        for name, payload in (
            ("workday_workers.json", workers),
            ("workday_persons.json", persons),
            ("workday_employment_events.json", events),
            ("_manifest.json", meta),
        ):
            (self.output_dir / name).write_text(json.dumps(payload, indent=2, default=str))

    def _build_workers_payload(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for p in self.persons:
            for e in p.employments:
                latest = e.profile_versions[-1]
                out.append({
                    "worker_id": e.worker_id,
                    "person_external_id": p.person_external_id,
                    "first_name": p.first_name,
                    "last_name": p.last_name,
                    "work_email": self._uniq_work_email_for_stint(p, e),
                    "personal_email": p.personal_email,
                    "hire_date": e.hire_date.isoformat(),
                    "termination_date": e.termination_date.isoformat() if e.termination_date else None,
                    "termination_reason": e.termination_reason,
                    "current_department": latest.department,
                    "current_function": latest.function,
                    "current_job_title": latest.job_title,
                    "current_job_level": latest.job_level,
                    "current_manager_worker_id": latest.manager_worker_id,
                    "current_worker_type": latest.worker_type,
                    "current_fte": latest.fte,
                    "current_location_city": latest.location_city,
                    "current_location_region": latest.location_region,
                    "current_location_country": latest.location_country,
                    "is_active": e.termination_date is None or e.termination_date > self.today,
                    "profile_history": [
                        {
                            "effective_date": pv.effective_date.isoformat(),
                            "department": pv.department,
                            "function": pv.function,
                            "job_title": pv.job_title,
                            "job_level": pv.job_level,
                            "manager_worker_id": pv.manager_worker_id,
                            "worker_type": pv.worker_type,
                            "fte": pv.fte,
                            "location_city": pv.location_city,
                            "location_region": pv.location_region,
                            "location_country": pv.location_country,
                            "event_type": pv.event_type,
                        }
                        for pv in e.profile_versions
                    ],
                    "source_updated_at": datetime.now(timezone.utc).isoformat(),
                })
        return out

    def _uniq_work_email_for_stint(self, p: Person, e: Employment) -> str:
        # Real Workday: same person on rehire gets a NEW work email. We mimic that.
        suffix = "" if p.employments.index(e) == 0 else f".r{p.employments.index(e)}"
        return f"{p.first_name.lower()}.{p.last_name.lower()}{suffix}@example.com"

    def _build_persons_payload(self) -> list[dict[str, Any]]:
        return [
            {
                "person_external_id": p.person_external_id,
                "first_name": p.first_name,
                "last_name": p.last_name,
                "personal_email": p.personal_email,
                "worker_ids": [e.worker_id for e in p.employments],
                "source_updated_at": datetime.now(timezone.utc).isoformat(),
            }
            for p in self.persons
        ]

    def _build_events_payload(self) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        seq = 0
        for p in self.persons:
            for e in p.employments:
                for pv in e.profile_versions:
                    seq += 1
                    out.append({
                        "event_id": f"E{seq:09d}",
                        "worker_id": e.worker_id,
                        "person_external_id": p.person_external_id,
                        "event_type": pv.event_type,
                        "effective_date": pv.effective_date.isoformat(),
                        "department": pv.department,
                        "function": pv.function,
                        "job_title": pv.job_title,
                        "job_level": pv.job_level,
                        "manager_worker_id": pv.manager_worker_id,
                        "termination_reason": e.termination_reason if pv.event_type == "termination" else None,
                        "source_updated_at": datetime.now(timezone.utc).isoformat(),
                    })
        return out


# -- CLI --------------------------------------------------------------------------------


@click.command()
@click.option("--employees", default=1000, help="Steady-state active employees at start.")
@click.option("--years", default=3, help="Years of history to simulate.")
@click.option("--seed", default=42, help="Random seed for reproducibility.")
@click.option("--output-dir", default=str(DEFAULT_OUTPUT_DIR), help="Output directory for JSON payloads.")
def main(employees: int, years: int, seed: int, output_dir: str) -> None:
    """Generate synthetic HRIS source payloads."""
    out = Path(output_dir)
    gen = Generator(employees=employees, years=years, seed=seed, output_dir=out)
    click.echo(f"Generating ~{employees} employees, {years}yr history, seed={seed}...")
    gen.run()
    click.echo(f"✓ Wrote payloads to {out}")
    manifest = json.loads((out / "_manifest.json").read_text())
    for k, v in manifest.items():
        click.echo(f"  {k}: {v}")


if __name__ == "__main__":
    main()
