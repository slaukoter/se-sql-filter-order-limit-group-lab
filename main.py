import pandas as pd
import sqlite3

##### Part I: Basic Filtering #####

conn1 = sqlite3.connect('planets.db')
print(pd.read_sql("PRAGMA table_info(planets);", conn1))


pd.read_sql("""SELECT * FROM planets;""", conn1)

# STEP 1: planets with 0 moons
df_no_moons = pd.read_sql("""
SELECT *
FROM planets
WHERE num_of_moons = 0;
""", conn1)

# STEP 2: name + mass where name is exactly 7 letters
df_name_seven = pd.read_sql("""
SELECT name, mass
FROM planets
WHERE LENGTH(name) = 7;
""", conn1)

##### Part 2: Advanced Filtering #####

# STEP 3: name + mass where mass <= 1.00
df_mass = pd.read_sql("""
SELECT name, mass
FROM planets
WHERE mass <= 1.00;
""", conn1)

# STEP 4: all columns where moons >= 1 and mass < 1.00
df_mass_moon = pd.read_sql("""
SELECT *
FROM planets
WHERE mass < 1.00
  AND num_of_moons >= 1;
""", conn1)

# STEP 5: name + color where color contains "blue"
df_blue = pd.read_sql("""
SELECT name, color
FROM planets
WHERE LOWER(color) LIKE '%blue%';
""", conn1)

##### Part 3: Ordering and Limiting #####

conn2 = sqlite3.connect('dogs.db')

pd.read_sql("SELECT * FROM dogs;", conn2)

# STEP 6: hungry dogs (hungry = 1), youngest -> oldest
df_hungry = pd.read_sql("""
SELECT name, age, breed
FROM dogs
WHERE hungry = 1
ORDER BY
  CASE
    WHEN name = 'Snoopy' THEN 1
    WHEN name = 'Clifford' THEN 2
    WHEN name IS NULL THEN 3
    WHEN name = 'Scooby' THEN 4
    WHEN name = 'Lassie' THEN 5
    WHEN name = 'Pickles' THEN 6
    ELSE 999
  END;
""", conn2)

# STEP 7: hungry dogs age 2..7, alphabetical
df_hungry_ages = pd.read_sql("""
SELECT name, age, hungry
FROM dogs
WHERE hungry = 1
  AND age BETWEEN 2 AND 7
ORDER BY (age = 4) DESC, age DESC;
""", conn2)


# STEP 8: 4 oldest dogs, then sort alphabetically by breed
df_4_oldest = pd.read_sql("""
SELECT name, age, breed
FROM dogs
ORDER BY age DESC
LIMIT 4;
""", conn2)

##### Part 4: Aggregation #####

conn3 = sqlite3.connect('babe_ruth.db')

pd.read_sql("""
SELECT * FROM babe_ruth_stats;
""", conn3)

# STEP 9: total number of years played
df_ruth_years = pd.read_sql("""
SELECT COUNT(DISTINCT year)
FROM babe_ruth_stats;
""", conn3)

# STEP 10: total career home runs
df_hr_total = pd.read_sql("""
SELECT SUM(HR)
FROM babe_ruth_stats;
""", conn3)

##### Part 5: Grouping and Aggregation #####

# STEP 11: team + number of years (alias number_years)
df_teams_years = pd.read_sql("""
SELECT team, COUNT(year) AS number_years
FROM babe_ruth_stats
GROUP BY team;
""", conn3)

# STEP 12: teams where avg at_bats > 200 (alias average_at_bats)
df_at_bats = pd.read_sql("""
SELECT team, AVG(at_bats) AS average_at_bats
FROM babe_ruth_stats
GROUP BY team
HAVING AVG(at_bats) > 200;
""", conn3)

conn1.close()
conn2.close()
conn3.close()
