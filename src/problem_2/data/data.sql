CREATE TABLE IF NOT EXISTS event_attributes (
    event_url VARCHAR(255),
    attribute VARCHAR(255),
    value TEXT
);

CREATE TABLE IF NOT EXISTS company_attributes (
    company_url VARCHAR(255),
    attribute VARCHAR(255),
    value TEXT
);

CREATE TABLE IF NOT EXISTS people_attributes (
    person_id SERIAL,
    attribute VARCHAR(255),
    value TEXT
);

TRUNCATE TABLE event_attributes, company_attributes, people_attributes;

INSERT INTO event_attributes (event_url, attribute, value) VALUES
('event1.com', 'event_start_date', '2025-01-15'),
('event1.com', 'event_city', 'Singapore'),
('event1.com', 'event_country', 'Singapore'),
('event1.com', 'event_name', 'Tech Summit'),
('event2.com', 'event_start_date', '2025-02-01'),
('event2.com', 'event_city', 'Tokyo'),
('event2.com', 'event_country', 'Japan'),
('event2.com', 'event_name', 'Future Tech Expo'),
('event3.com', 'event_start_date', '2025-01-20'),
('event3.com', 'event_city', 'Singapore'),
('event3.com', 'event_country', 'Singapore'),
('event3.com', 'event_name', 'AI Conference');

INSERT INTO company_attributes (company_url, attribute, value) VALUES
('company1.com', 'company_name', 'Tech Innovators Inc.'),
('company1.com', 'company_country', 'USA'),
('company1.com', 'company_industry', 'Software'),
('company1.com', 'company_event_url', 'event1.com'),
('company2.com', 'company_name', 'Global IT Solutions'),
('company2.com', 'company_country', 'India'),
('company2.com', 'company_industry', 'IT'),
('company2.com', 'company_event_url', 'event2.com'),
('company3.com', 'company_name', 'Future Tech'),
('company3.com', 'company_country', 'Japan'),
('company3.com', 'company_industry', 'Software'),
('company3.com', 'company_event_url', 'event3.com');

INSERT INTO people_attributes (person_id, attribute, value) VALUES
(1, 'person_first_name', 'John'),
(1, 'person_last_name', 'Doe'),
(1, 'person_email', 'john.doe@company1.com'),
(1, 'person_seniority', 'director'),
(1, 'person_department', 'Engineering'),
(1, 'person_company_url', 'company1.com'),
(2, 'person_first_name', 'Jane'),
(2, 'person_last_name', 'Smith'),
(2, 'person_email', 'jane.smith@company2.com'),
(2, 'person_seniority', 'manager'),
(2, 'person_department', 'Marketing'),
(2, 'person_company_url', 'company2.com'),
(3, 'person_first_name', 'Alice'),
(3, 'person_last_name', 'Johnson'),
(3, 'person_email', 'alice.johnson@company3.com'),
(3, 'person_seniority', 'director'),
(3, 'person_department', 'Sales'),
(3, 'person_company_url', 'company3.com');

CREATE INDEX idx_event_url ON event_attributes(event_url);
CREATE INDEX idx_company_url ON company_attributes(company_url);
CREATE INDEX idx_person_id ON people_attributes(person_id);
