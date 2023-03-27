--creates Table covid_deaths
CREATE TABLE [dbo].[covid_deaths] (
[iso_code] nvarchar(255),
[continent] nvarchar(255),
[location] nvarchar(255),
[date] datetime,
[population] nvarchar(255),
[total_cases] float,
[new_cases] float,
[new_cases_smoothed] nvarchar(255),
[total_deaths] float,
[new_deaths] float,
[new_deaths_smoothed] nvarchar(255),
[total_cases_per_million] float,
[new_cases_per_million] float,
[new_cases_smoothed_per_million] nvarchar(255),
[total_deaths_per_million] float,
[new_deaths_per_million] float,
[new_deaths_smoothed_per_million] nvarchar(255),
[reproduction_rate] nvarchar(255),
[icu_patients] nvarchar(255),
[icu_patients_per_million] nvarchar(255),
[hosp_patients] nvarchar(255),
[hosp_patients_per_million] nvarchar(255),
[weekly_icu_admissions] nvarchar(255),
[weekly_icu_admissions_per_million] nvarchar(255),
[weekly_hosp_admissions] nvarchar(255),
[weekly_hosp_admissions_per_million] nvarchar(255)
)
--creates covid_vactionations tables
CREATE TABLE [dbo].[covid_vaccinations] (
[iso_code] nvarchar(255),
[continent] nvarchar(255),
[location] nvarchar(255),
[date] datetime,
[total_tests] nvarchar(255),
[new_tests] nvarchar(255),
[total_tests_per_thousand] nvarchar(255),
[new_tests_per_thousand] nvarchar(255),
[new_tests_smoothed] nvarchar(255),
[new_tests_smoothed_per_thousand] nvarchar(255),
[positive_rate] nvarchar(255),
[tests_per_case] nvarchar(255),
[tests_units] nvarchar(255),
[total_vaccinations] nvarchar(255),
[people_vaccinated] nvarchar(255),
[people_fully_vaccinated] nvarchar(255),
[total_boosters] nvarchar(255),
[new_vaccinations] nvarchar(255),
[new_vaccinations_smoothed] nvarchar(255),
[total_vaccinations_per_hundred] nvarchar(255),
[people_vaccinated_per_hundred] nvarchar(255),
[people_fully_vaccinated_per_hundred] nvarchar(255),
[total_boosters_per_hundred] nvarchar(255),
[new_vaccinations_smoothed_per_million] nvarchar(255),
[new_people_vaccinated_smoothed] nvarchar(255),
[new_people_vaccinated_smoothed_per_hundred] nvarchar(255),
[stringency_index] float,
[population_density] float,
[median_age] float,
[aged_65_older] float,
[aged_70_older] float,
[gdp_per_capita] nvarchar(255),
[extreme_poverty] float,
[cardiovasc_death_rate] float,
[diabetes_prevalence] datetime,
[female_smokers] nvarchar(255),
[male_smokers] float,
[handwashing_facilities] float,
[hospital_beds_per_thousand] float,
[life_expectancy] float,
[human_development_index] float,
[excess_mortality_cumulative_absolute] nvarchar(255),
[excess_mortality_cumulative] nvarchar(255),
[excess_mortality] nvarchar(255),
[excess_mortality_cumulative_per_million] nvarchar(255)
)


--import from csv file
--LOAD DATA LOCAL INFILE  
----'e: datasets\covid_deaths.csv '
--INTO TABLE covid_deaths  
--FIELDS TERMINATED BY ',' 
--ENCLOSED BY '"'


--import from csv file
--LOAD DATA LOCAL INFILE  
----'e: datasets\covid_vaccinations.csv '
--INTO TABLE covid_deaths  
--FIELDS TERMINATED BY ',' 
--ENCLOSED BY '"'




--Select Data that we are going to be using

select location, date , total_cases, new_cases, total_deaths, population
from PortfolioProject..covid_deaths
order by 1,2

--likely hood of death from covid in the united stated
select 
location, date,total_cases, total_deaths, isnull(((total_deaths/nullif(total_cases,0))*100),0) as percentOfDeath
from PortfolioProject..covid_deaths
where location like '%states%'
group by location,date, total_cases,total_deaths



--shows what percentage got covid
select 
location, date,total_cases, population, isnull(((total_cases/nullif(population,0))*100),0) as percentofPopulation
from PortfolioProject..Covid_Deaths
where location like '%states%'
group by location,date, population,total_cases

--LOOKING AT MOST INFECTON RATES COMPARED TO  POPULATION
select 
location, population, MAX(total_cases) as HighestInfectionCount, MAX(isnull(((total_cases/nullif(population,0))*100),0)) as percentofPopulationInfection
from PortfolioProject..Covid_Deaths
--where location like '%states%'
group by location, population,total_cases
order by  percentofPopulationInfection desc

--How many people died? highest death count per poulation
select 
location, MAX(cast(total_deaths as bigint)) as TotalDeaths
from PortfolioProject..Covid_Deaths
where continent is not NULL
group by location
order by  TotalDeaths desc

--now by continent
select 
location, MAX(cast(total_deaths as bigint)) as TotalDeaths
from PortfolioProject..Covid_Deaths
where continent is not NULL
group by location
order by  TotalDeaths desc

--showing the continets with the highest death count

select 
continent, MAX(cast(total_deaths as bigint)) as TotalDeaths
from PortfolioProject..Covid_Deaths
where continent is not NULL
group by continent
order by  TotalDeaths desc

-- Global numbers by date
select date,sum(new_cases) as total_cases, SUM(cast(new_deaths as bigint)) as total_deaths,
isnull((SUM(cast(new_deaths as bigint))/nullif(sum(new_cases),0)*100),0) as deathPercentage
from PortfolioProject..covid_deaths
where continent is not null
group by date

--global death percentage
select sum(new_cases) as total_cases, SUM(cast(new_deaths as bigint)) as total_deaths,
isnull((SUM(cast(new_deaths as bigint))/nullif(sum(new_cases),0)*100),0) as deathPercentage
from PortfolioProject..covid_deaths
where continent is not null

--total population vs vacinoations
with PopvsVac (continent, location,date,population,new_vaccinations,RollingPeopleVaccinated)
as
(
select dea.continent, dea.location,dea.date,dea.population,vac.new_vaccinations 
, SUM(cast(vac.new_vaccinations as bigint)) over (partition by dea.location order by dea.location,dea.date) as RollingPeopleVaccinated

from PortfolioProject.. Covid_Deaths dea
join PortfolioProject.. Covid_Vaccinations vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null
--order by 2,3
)
select *, (RollingPeopleVaccinated/population)*100
from PopvsVac
--Use CTE

--temp table
drop table if exists  #percentPopulatationVaccinated
create table #percentPopulatationVaccinated
(
continent nvarchar(255),
location nvarchar(255),
date datetime,
population numeric, 
new_vaccinations numeric,
RollingPeopleVaccinated numeric
)
insert into #percentPopulatationVaccinated
select dea.continent, dea.location,dea.date,dea.population,vac.new_vaccinations 
, SUM(cast(vac.new_vaccinations as bigint)) over (partition by dea.location order by dea.location,dea.date) as RollingPeopleVaccinated

from PortfolioProject.. Covid_Deaths dea
join PortfolioProject.. Covid_Vaccinations vac
	on dea.location = vac.location
	and dea.date = vac.date
--where dea.continent is not null

select *, (RollingPeopleVaccinated/population)*100
from #percentPopulatationVaccinated

--create view to store data for later visualizations
create view percentPopulatationVaccinated as
select dea.continent, dea.location,dea.date,dea.population,vac.new_vaccinations 
, SUM(cast(vac.new_vaccinations as bigint)) over (partition by dea.location order by dea.location,dea.date) as RollingPeopleVaccinated
from PortfolioProject.. Covid_Deaths dea
join PortfolioProject.. Covid_Vaccinations vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null