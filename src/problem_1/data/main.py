import polars as pl
from datetime import date
from typing import Dict


class Dataset:
    def __init__(self) -> None:
        self.num_companies = 30
        self.num_events = 10
        self.employees_per_company = 5

        # Fixed lists for deterministic choices
        self.industries = [
            "Tech",
            "Finance",
            "Healthcare",
            "Retail",
            "Manufacturing",
            "Energy",
            "Real Estate",
            "Education",
            "Entertainment",
            "Automotive",
        ]
        self.countries = [
            "USA",
            "UK",
            "Germany",
            "Japan",
            "Australia",
            "France",
            "Canada",
            "Singapore",
            "UAE",
            "Brazil",
        ]
        self.cities = [
            "New York",
            "London",
            "Paris",
            "Tokyo",
            "Sydney",
            "Berlin",
            "San Francisco",
            "Toronto",
            "Singapore",
            "Dubai",
        ]
        self.seniorities = ["Director", "C-Level", "Manager", "Engineer", "VP"]
        self.departments = ["Sales", "Engineering", "Marketing", "HR", "Finance"]
        self.event_relations = ["Sponsor", "Attendee"]

        self.events_df = pl.DataFrame(
            {
                "event_url": [f"event-{i}" for i in range(1, self.num_events + 1)],
                "event_name": [f"Event {i}" for i in range(1, self.num_events + 1)],
                "event_start_date": [
                    date(2023, i, 1) for i in range(1, self.num_events + 1)
                ],
                "event_city": self.cities[: self.num_events],
                "event_country": self.countries[: self.num_events],
                "event_industry": self.industries[: self.num_events],
            }
        )

        self.companies_df = pl.DataFrame(
            {
                "company_url": [
                    f"company-{i}" for i in range(1, self.num_companies + 1)
                ],
                "company_name": [
                    f"Company {i}" for i in range(1, self.num_companies + 1)
                ],
                "company_industry": [
                    self.industries[i % len(self.industries)]
                    for i in range(self.num_companies)
                ],
                "company_revenue": [
                    1000000 + i * 100000 for i in range(self.num_companies)
                ],
                "company_country": [
                    self.countries[i % len(self.countries)]
                    for i in range(self.num_companies)
                ],
            }
        )

        self.event_attendees_df = pl.DataFrame(
            {
                "event_url": [
                    f"event-{i}"
                    for i in range(1, self.num_events + 1)
                    for _ in range(10)
                ],
                "company_url": [
                    f"company-{1 + (i % self.num_companies)}"
                    for i in range(self.num_events * 10)
                ],
                "company_relation_to_event": [
                    self.event_relations[i % len(self.event_relations)]
                    for i in range(self.num_events * 10)
                ],
            }
        )

        self.company_employees_df = pl.DataFrame(
            {
                "company_url": [
                    f"company-{1 + (i // self.employees_per_company)}"
                    for i in range(self.num_companies * self.employees_per_company)
                ],
                "person_id": [
                    f"person-{i+1}"
                    for i in range(self.num_companies * self.employees_per_company)
                ],
                "person_first_name": [
                    f"FirstName{i+1}"
                    for i in range(self.num_companies * self.employees_per_company)
                ],
                "person_last_name": [
                    f"LastName{i+1}"
                    for i in range(self.num_companies * self.employees_per_company)
                ],
                "person_email": [
                    f"person{i+1}@company{1 + (i // self.employees_per_company)}.com"
                    for i in range(self.num_companies * self.employees_per_company)
                ],
                "person_city": [
                    self.cities[i % len(self.cities)]
                    for i in range(self.num_companies * self.employees_per_company)
                ],
                "person_country": [
                    self.countries[i % len(self.countries)]
                    for i in range(self.num_companies * self.employees_per_company)
                ],
                "person_seniority": [
                    self.seniorities[i % len(self.seniorities)]
                    for i in range(self.num_companies * self.employees_per_company)
                ],
                "person_department": [
                    self.departments[i % len(self.departments)]
                    for i in range(self.num_companies * self.employees_per_company)
                ],
            }
        )

    def get(self) -> Dict[str, pl.DataFrame]:
        return {
            "events": self.events_df,
            "companies": self.companies_df,
            "event_attendees": self.event_attendees_df,
            "company_employees": self.company_employees_df,
        }
