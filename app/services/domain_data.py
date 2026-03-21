"""Domain-specific policy guidance data and mapping functions.

Maps bill subjects to policy domains with Idaho-specific analysis guidance.
"""
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


# Map subject names to our domain categories
SUBJECT_TO_DOMAIN = {
    # Education
    "Education": "education",
    "Higher Education": "education",
    "Elementary & Secondary Education": "education",
    "Educational Facilities": "education",
    "Special Education": "education",
    "Preschool": "education",
    "School Finance": "education",
    "Teachers": "education",
    "Charter Schools": "education",
    "Vocational Education": "education",

    # Healthcare
    "Health": "healthcare",
    "Healthcare": "healthcare",
    "Health Care": "healthcare",
    "Mental Health": "healthcare",
    "Medicaid": "healthcare",
    "Medicare": "healthcare",
    "Public Health": "healthcare",
    "Health Insurance": "healthcare",
    "Pharmaceuticals": "healthcare",
    "Nursing": "healthcare",
    "Physicians": "healthcare",
    "Hospitals": "healthcare",
    "Health Facilities": "healthcare",
    "Disease Control": "healthcare",
    "Substance Abuse": "healthcare",

    # Taxation & Revenue
    "Taxation": "taxation",
    "Revenue": "taxation",
    "Income Tax": "taxation",
    "Sales Tax": "taxation",
    "Property Tax": "taxation",
    "Tax Credits": "taxation",
    "Tax Exemptions": "taxation",
    "Corporate Tax": "taxation",

    # Agriculture
    "Agriculture": "agriculture",
    "Farming": "agriculture",
    "Livestock": "agriculture",
    "Dairy": "agriculture",
    "Agricultural Marketing": "agriculture",
    "Agricultural Products": "agriculture",
    "Ranching": "agriculture",
    "Food": "agriculture",
    "Irrigation": "agriculture",

    # Water & Natural Resources
    "Water": "natural_resources",
    "Water Resources": "natural_resources",
    "Water Rights": "natural_resources",
    "Natural Resources": "natural_resources",
    "Environment": "natural_resources",
    "Environmental Protection": "natural_resources",
    "Fish & Game": "natural_resources",
    "Wildlife": "natural_resources",
    "Forestry": "natural_resources",
    "Mining": "natural_resources",
    "Public Lands": "natural_resources",
    "Parks & Recreation": "natural_resources",
    "Air Quality": "natural_resources",
    "Pollution": "natural_resources",

    # Transportation
    "Transportation": "transportation",
    "Highways": "transportation",
    "Roads": "transportation",
    "Motor Vehicles": "transportation",
    "Traffic": "transportation",
    "Drivers Licenses": "transportation",
    "Railroads": "transportation",
    "Aviation": "transportation",
    "Public Transit": "transportation",

    # Public Safety & Criminal Justice
    "Criminal Justice": "public_safety",
    "Crime": "public_safety",
    "Corrections": "public_safety",
    "Law Enforcement": "public_safety",
    "Police": "public_safety",
    "Prisons": "public_safety",
    "Probation & Parole": "public_safety",
    "Sentencing": "public_safety",
    "Courts": "public_safety",
    "Judiciary": "public_safety",
    "Domestic Violence": "public_safety",
    "Firearms": "public_safety",
    "Emergency Management": "public_safety",
    "Fire Protection": "public_safety",
    "Juvenile Justice": "public_safety",

    # State Government & Administration
    "State Government": "state_government",
    "Government Administration": "state_government",
    "State Agencies": "state_government",
    "Public Employees": "state_government",
    "State Personnel": "state_government",
    "Retirement": "state_government",
    "Pensions": "state_government",
    "PERSI": "state_government",
    "Procurement": "state_government",
    "Public Records": "state_government",
    "Elections": "state_government",
    "Campaign Finance": "state_government",
    "Ethics": "state_government",
    "Lobbying": "state_government",

    # Local Government
    "Local Government": "local_government",
    "Counties": "local_government",
    "Cities": "local_government",
    "Municipal Government": "local_government",
    "Special Districts": "local_government",
    "Zoning": "local_government",
    "Land Use": "local_government",
    "Planning": "local_government",
    "Urban Development": "local_government",

    # Business & Economic Development
    "Commerce": "business",
    "Business": "business",
    "Economic Development": "business",
    "Corporations": "business",
    "Small Business": "business",
    "Banking": "business",
    "Finance": "business",
    "Insurance": "business",
    "Securities": "business",
    "Consumer Protection": "business",
    "Licensing": "business",
    "Professional Regulation": "business",
    "Telecommunications": "business",
    "Technology": "business",
    "Utilities": "business",

    # Labor & Employment
    "Labor": "labor",
    "Employment": "labor",
    "Workers Compensation": "labor",
    "Unemployment": "labor",
    "Wages": "labor",
    "Workplace Safety": "labor",
    "Unions": "labor",
    "Collective Bargaining": "labor",

    # Social Services
    "Social Services": "social_services",
    "Welfare": "social_services",
    "Public Assistance": "social_services",
    "Child Welfare": "social_services",
    "Children": "social_services",
    "Families": "social_services",
    "Aging": "social_services",
    "Disabilities": "social_services",
    "Housing": "social_services",
    "Homelessness": "social_services",
    "Veterans": "social_services",

    # Appropriations & Budget
    "Appropriations": "appropriations",
    "Budget": "appropriations",
    "State Budget": "appropriations",
    "Fiscal": "appropriations",
}

# Detailed domain-specific analysis guidance
DOMAIN_GUIDANCE = {
    "education": """
EDUCATION POLICY DOMAIN
This bill relates to education policy. Apply these Idaho-specific considerations:

KEY STAKEHOLDERS TO ADDRESS:
- School districts (115 districts in Idaho, varying greatly in size and resources)
- Teachers and school administrators
- Students and families
- Idaho State Board of Education
- Idaho State Department of Education
- Local school boards
- Charter school operators (if applicable)
- Higher education institutions (if applicable)

IDAHO EDUCATION CONTEXT:
- Idaho's public school funding formula is based on Average Daily Attendance (ADA) and support units
- The state provides approximately 65-70% of K-12 funding; local property taxes and federal funds cover the rest
- Idaho has significant rural/urban disparities in educational resources and outcomes
- Teacher recruitment and retention is a persistent challenge, especially in rural areas
- Idaho's literacy and math proficiency rates lag national averages

ANALYSIS FOCUS:
- How does this affect the funding formula or per-pupil allocations?
- What is the impact on rural vs. urban districts?
- Does this create unfunded mandates for local districts?
- How does this affect teacher workload, certification, or compensation?
- What are the implementation requirements for the State Department of Education?
- Are there accountability or reporting requirements?
- How does this interact with Idaho's career-technical education system?

QUESTIONS TO PROBE:
- What is the fiscal impact on the state general fund vs. local property taxes?
- How will small/rural districts with limited administrative capacity implement this?
- What is the timeline for implementation and is it realistic?
- Are there waivers or exceptions for districts that cannot comply?
""",

    "healthcare": """
HEALTHCARE POLICY DOMAIN
This bill relates to healthcare policy. Apply these Idaho-specific considerations:

KEY STAKEHOLDERS TO ADDRESS:
- Patients and families
- Healthcare providers (physicians, nurses, specialists)
- Hospitals and health systems (critical access hospitals in rural areas)
- Idaho Department of Health and Welfare
- Health insurers and managed care organizations
- Medicaid recipients and providers
- Mental health and substance abuse treatment providers
- Long-term care facilities
- Public health districts (7 regional districts in Idaho)

IDAHO HEALTHCARE CONTEXT:
- Idaho has significant healthcare workforce shortages, especially in rural areas
- Many rural communities rely on Critical Access Hospitals
- Idaho expanded Medicaid in 2020 via ballot initiative; approximately 120,000+ Idahoans enrolled
- Behavioral health services are severely limited in most of the state
- Idaho has one of the highest rates of uninsured residents among states
- Scope of practice debates (nurse practitioners, physician assistants) are ongoing

ANALYSIS FOCUS:
- How does this affect Medicaid eligibility, coverage, or reimbursement rates?
- What is the impact on rural healthcare access and Critical Access Hospitals?
- Does this affect healthcare workforce licensing or scope of practice?
- How does this interact with federal healthcare requirements (CMS, ACA)?
- What is the fiscal impact on the Department of Health and Welfare?
- Are there federal matching fund implications?

QUESTIONS TO PROBE:
- Will this affect Idaho's federal Medicaid matching rate (currently ~70% federal)?
- How will this impact healthcare access in underserved rural areas?
- What is the impact on Idaho's 7 public health districts?
- Does this require CMS approval or create compliance risks?
""",

    "taxation": """
TAXATION & REVENUE POLICY DOMAIN
This bill relates to taxation or state revenue. Apply these Idaho-specific considerations:

KEY STAKEHOLDERS TO ADDRESS:
- Idaho taxpayers (individual and business)
- Idaho State Tax Commission
- Local governments that share state revenues
- Businesses affected by tax changes
- Tax practitioners and accountants
- Economic development interests

IDAHO TAX CONTEXT:
- Idaho has a graduated income tax (recently flattened to 5.8% flat rate)
- Sales tax is 6% with limited exemptions (groceries are taxed)
- Property tax is administered locally with state oversight
- Idaho has no inheritance or estate tax
- The state maintains a Budget Stabilization Fund (rainy day fund)
- Revenue sharing with local governments is formula-based

ANALYSIS FOCUS:
- What is the estimated revenue impact (positive or negative)?
- Who bears the tax burden or receives the benefit? (income distribution analysis)
- How does this affect Idaho's tax competitiveness with neighboring states?
- Does this create complexity or compliance burden for taxpayers?
- What is the impact on local government revenue sharing?
- Is this a one-time or ongoing revenue impact?
- How does this interact with the Budget Stabilization Fund requirements?

QUESTIONS TO PROBE:
- What is the fiscal note estimate and how reliable is the methodology?
- Is this revenue impact sustainable or does it create structural budget issues?
- How does this affect different income levels or business sizes?
- What are the administrative costs for the Tax Commission to implement?
""",

    "agriculture": """
AGRICULTURE POLICY DOMAIN
This bill relates to agriculture policy. Apply these Idaho-specific considerations:

KEY STAKEHOLDERS TO ADDRESS:
- Farmers and ranchers
- Agricultural cooperatives and commodity groups
- Idaho State Department of Agriculture
- Food processors and agricultural businesses
- Irrigation districts and water users
- Farm workers and agricultural labor
- Rural communities dependent on agriculture
- Dairy industry (Idaho is #3 in US milk production)

IDAHO AGRICULTURE CONTEXT:
- Agriculture contributes $8+ billion annually to Idaho's economy
- Idaho is #1 in potato production, #3 in dairy, major producer of wheat, barley, sugar beets
- Approximately 24,000 farms and ranches operate in Idaho
- Water rights and irrigation are critical (Snake River system)
- Agricultural labor availability is a persistent concern
- Right-to-farm laws protect agricultural operations from nuisance claims

ANALYSIS FOCUS:
- How does this affect farm/ranch operations and profitability?
- What is the impact on water rights or irrigation infrastructure?
- Does this affect agricultural labor or H-2A visa workers?
- How does this interact with federal agricultural programs (USDA, FSA)?
- What is the impact on Idaho's major commodities (dairy, potatoes, cattle, wheat)?
- Does this affect food safety or agricultural inspection programs?
- Are there environmental or water quality implications?

QUESTIONS TO PROBE:
- How will this affect small vs. large agricultural operations differently?
- What is the impact on Idaho's agricultural export markets?
- Does this create regulatory burden or reduce it?
- How does this affect the Idaho State Department of Agriculture's workload?
""",

    "natural_resources": """
NATURAL RESOURCES & ENVIRONMENT POLICY DOMAIN
This bill relates to natural resources or environmental policy. Apply these Idaho-specific considerations:

KEY STAKEHOLDERS TO ADDRESS:
- Water users (agricultural, municipal, industrial, recreational)
- Idaho Department of Water Resources
- Idaho Department of Environmental Quality
- Idaho Department of Fish and Game
- Idaho Department of Lands
- Mining and timber industries
- Outdoor recreation industry and sportsmen
- Environmental and conservation organizations
- Federal land management agencies (USFS, BLM - 62% of Idaho is federal land)

IDAHO NATURAL RESOURCES CONTEXT:
- Water is Idaho's most critical natural resource; the prior appropriation doctrine governs water rights
- The Snake River Basin Adjudication established most water rights in southern Idaho
- 62% of Idaho's land is federally managed (ongoing state-federal tensions)
- Idaho's timber and mining industries are significant economic drivers
- Fish and wildlife management involves complex federal/state/tribal coordination
- The Idaho Department of Environmental Quality oversees air and water quality

ANALYSIS FOCUS:
- How does this affect water rights or the prior appropriation system?
- What is the impact on Idaho Department of Water Resources administration?
- Does this affect state interaction with federal land management?
- What are the implications for fish and wildlife habitat?
- How does this affect the timber, mining, or outdoor recreation industries?
- Are there Clean Water Act or Clean Air Act compliance implications?
- Does this affect water quality or drinking water standards?

QUESTIONS TO PROBE:
- How does this interact with existing water rights and adjudication decrees?
- What is the impact on endangered species (salmon, steelhead, sage grouse)?
- Does this create conflict with federal environmental requirements?
- How does this affect Idaho's relationship with downstream states (water compacts)?
""",

    "transportation": """
TRANSPORTATION POLICY DOMAIN
This bill relates to transportation policy. Apply these Idaho-specific considerations:

KEY STAKEHOLDERS TO ADDRESS:
- Idaho Transportation Department (ITD)
- Local highway districts (over 60 in Idaho)
- Motor vehicle operators and commercial drivers
- Trucking and freight industry
- Law enforcement (traffic enforcement)
- Cities and counties (local roads)
- Idaho Transportation Board

IDAHO TRANSPORTATION CONTEXT:
- Idaho has approximately 12,500 miles of state highways
- Local highway districts (unique to Idaho) maintain most local roads
- The Highway Distribution Account allocates fuel tax revenue
- Idaho's transportation infrastructure faces significant maintenance backlogs
- Commercial truck traffic on I-84 and US-95 is heavy (agricultural/freight corridor)
- Rural road maintenance is a significant challenge

ANALYSIS FOCUS:
- What is the fiscal impact on the Highway Distribution Account?
- How are revenues distributed between state, local highway districts, and cities?
- Does this affect vehicle registration fees, fuel taxes, or other transportation revenue?
- What is the impact on ITD operations or staffing?
- Are there federal highway funding implications?
- How does this affect commercial vehicle regulations?
- What is the impact on local highway districts?

QUESTIONS TO PROBE:
- Does this affect Idaho's ability to match federal transportation funds?
- How will this impact the state's highway maintenance backlog?
- What is the effect on local highway districts' funding and operations?
- Are there safety implications for Idaho's highways?
""",

    "public_safety": """
PUBLIC SAFETY & CRIMINAL JUSTICE POLICY DOMAIN
This bill relates to public safety or criminal justice. Apply these Idaho-specific considerations:

KEY STAKEHOLDERS TO ADDRESS:
- Law enforcement agencies (Idaho State Police, county sheriffs, local police)
- Idaho Department of Correction
- County jails and sheriffs
- Courts and judiciary (district courts, magistrate courts)
- Prosecutors and public defenders
- Crime victims and victim advocacy organizations
- Probation and parole officers
- Idaho Commission of Pardons and Parole

IDAHO CRIMINAL JUSTICE CONTEXT:
- Idaho's prison population has grown significantly; facilities are near capacity
- County jails often house state prisoners (cost-sharing agreements)
- Idaho has mandatory minimum sentences for certain drug and violent offenses
- The Justice Reinvestment Initiative aimed to reduce recidivism and costs
- Rural counties struggle with public defender and prosecution resources
- Idaho's drug courts and problem-solving courts have expanded

ANALYSIS FOCUS:
- What is the fiscal impact on the Department of Correction?
- Does this create new crimes or modify sentencing (prison population impact)?
- How does this affect county jail populations and costs?
- What is the impact on courts, prosecutors, and public defenders?
- Are there victims' rights implications?
- How does this interact with Justice Reinvestment principles?
- Does this affect law enforcement training or certification requirements?

QUESTIONS TO PROBE:
- What is the projected impact on prison population and costs?
- How will this affect county jail populations and county budgets?
- Does this create unfunded mandates for local law enforcement?
- What are the due process implications?
""",

    "state_government": """
STATE GOVERNMENT & ADMINISTRATION POLICY DOMAIN
This bill relates to state government operations. Apply these Idaho-specific considerations:

KEY STAKEHOLDERS TO ADDRESS:
- State agencies and departments affected
- State employees and PERSI (retirement system)
- Division of Human Resources
- Division of Purchasing
- Office of the Governor
- Legislative Services Office
- State Controller and Treasurer
- Taxpayers (as funders of state operations)

IDAHO STATE GOVERNMENT CONTEXT:
- Idaho state government employs approximately 24,000 workers
- PERSI (Public Employee Retirement System of Idaho) covers state and many local employees
- Idaho has a part-time citizen legislature (sessions typically 3-4 months)
- State agency budgets are set through the JFAC appropriations process
- Idaho has maintained conservative fiscal management with balanced budgets
- The Governor's office has significant budget and appointment authority

ANALYSIS FOCUS:
- What is the fiscal impact on state agency budgets?
- Does this affect state employee compensation, benefits, or retirement?
- How does this change agency authority, structure, or reporting?
- What is the impact on PERSI or state employee retirement benefits?
- Does this affect procurement or contracting processes?
- Are there public records or transparency implications?
- How does this affect legislative oversight of executive agencies?

QUESTIONS TO PROBE:
- What is the ongoing vs. one-time cost to the state general fund?
- Does this require new FTEs (full-time employees) or can existing staff absorb it?
- How does this affect the balance of power between branches of government?
- Are there administrative rule-making implications?
""",

    "local_government": """
LOCAL GOVERNMENT POLICY DOMAIN
This bill relates to local government. Apply these Idaho-specific considerations:

KEY STAKEHOLDERS TO ADDRESS:
- County commissioners and county governments (44 counties)
- City councils and municipal governments
- Association of Idaho Cities
- Idaho Association of Counties
- Special districts (fire, recreation, cemetery, etc.)
- Local government employees
- Property taxpayers
- Urban renewal agencies
- Planning and zoning commissions

IDAHO LOCAL GOVERNMENT CONTEXT:
- Idaho has 44 counties with varying populations (Ada County ~500K, Custer County ~4K)
- Cities operate under general law or home rule authority
- Property tax is the primary local revenue source (with state-imposed caps)
- Local highway districts are unique to Idaho
- Urban renewal/tax increment financing is controversial
- Many local governments share services due to limited resources

ANALYSIS FOCUS:
- Does this create unfunded mandates for local governments?
- How does this affect local government revenue or property tax authority?
- What is the impact on county vs. city responsibilities?
- Does this affect local land use or zoning authority?
- How does this interact with property tax limitations?
- Are there urban renewal or TIF implications?
- What is the impact on small/rural local governments with limited staff?

QUESTIONS TO PROBE:
- Does this create new costs that local governments must absorb?
- How will small counties with limited resources implement this?
- Does this preempt local decision-making authority?
- What is the impact on local property tax rates?
""",

    "business": """
BUSINESS & ECONOMIC DEVELOPMENT POLICY DOMAIN
This bill relates to business regulation or economic development. Apply these Idaho-specific considerations:

KEY STAKEHOLDERS TO ADDRESS:
- Idaho businesses (small, medium, large)
- Idaho Commerce Department
- Professional licensing boards
- Idaho Small Business Development Center
- Chambers of commerce
- Industry-specific trade associations
- Consumers
- Workers in affected industries

IDAHO BUSINESS CONTEXT:
- Idaho consistently ranks as one of the fastest-growing states
- Small businesses (under 500 employees) comprise 99% of Idaho businesses
- Technology sector has grown significantly (Boise area, Micron, HP, others)
- Idaho emphasizes limited regulation and business-friendly environment
- Occupational licensing affects approximately 20% of Idaho's workforce
- Idaho does not have a state minimum wage above federal level

ANALYSIS FOCUS:
- Does this increase or decrease regulatory burden on businesses?
- What is the impact on small vs. large businesses?
- How does this affect occupational licensing or barriers to entry?
- Does this affect consumer protection or create liability issues?
- What is the economic development impact?
- How does this affect Idaho's business competitiveness?
- Are there workforce development implications?

QUESTIONS TO PROBE:
- What is the compliance cost for affected businesses?
- How does this affect Idaho's regulatory competitiveness with neighboring states?
- Does this create barriers to entry for new businesses or workers?
- What is the consumer protection vs. business flexibility tradeoff?
""",

    "labor": """
LABOR & EMPLOYMENT POLICY DOMAIN
This bill relates to labor and employment. Apply these Idaho-specific considerations:

KEY STAKEHOLDERS TO ADDRESS:
- Idaho workers and job seekers
- Idaho employers
- Idaho Department of Labor
- Industrial Commission (workers' compensation)
- Workforce development councils
- Organized labor (limited presence in Idaho)
- Temporary staffing agencies
- Unemployment insurance system

IDAHO LABOR CONTEXT:
- Idaho is a right-to-work state (since 1985)
- Idaho follows federal minimum wage ($7.25/hour)
- Unemployment rate historically below national average
- Workers' compensation is administered by the Industrial Commission
- Workforce shortages exist in healthcare, construction, skilled trades
- Idaho has limited union membership (approximately 4-5% of workforce)

ANALYSIS FOCUS:
- How does this affect worker rights or protections?
- What is the impact on employer obligations or costs?
- Does this affect workers' compensation premiums or benefits?
- How does this interact with unemployment insurance?
- What is the impact on workforce development programs?
- Does this affect workplace safety requirements?
- How does this affect the Department of Labor's operations?

QUESTIONS TO PROBE:
- What is the cost impact on employers, especially small businesses?
- How does this affect Idaho's workforce competitiveness?
- Does this affect workers' compensation fund solvency?
- What is the impact on unemployment insurance trust fund?
""",

    "social_services": """
SOCIAL SERVICES POLICY DOMAIN
This bill relates to social services or human services. Apply these Idaho-specific considerations:

KEY STAKEHOLDERS TO ADDRESS:
- Idaho Department of Health and Welfare
- Service recipients and families
- Social service providers (nonprofit and for-profit)
- Child welfare workers and foster families
- Aging and disability service providers
- Area Agencies on Aging
- Community action agencies
- Veterans service organizations

IDAHO SOCIAL SERVICES CONTEXT:
- Department of Health and Welfare is Idaho's largest state agency
- Child welfare system faces caseworker shortage and high turnover
- Idaho's senior population is growing faster than the national average
- Affordable housing shortage affects low-income Idahoans statewide
- Mental health and substance abuse services are significantly underfunded
- Faith-based organizations provide substantial social services

ANALYSIS FOCUS:
- What is the fiscal impact on the Department of Health and Welfare?
- How does this affect eligibility or benefits for vulnerable populations?
- Are there federal funding or matching implications?
- What is the impact on child welfare system and foster care?
- How does this affect services for seniors and people with disabilities?
- Does this affect service provider reimbursement rates?
- What is the impact on Idaho's most vulnerable citizens?

QUESTIONS TO PROBE:
- Does this affect federal matching funds for social services?
- What is the impact on families at or below the poverty level?
- How will this affect DHW's caseload and staffing needs?
- Are there civil rights or due process implications for service recipients?
""",

    "appropriations": """
APPROPRIATIONS & BUDGET POLICY DOMAIN
This bill is an appropriation or budget bill. Apply these Idaho-specific considerations:

KEY STAKEHOLDERS TO ADDRESS:
- Receiving agency or program
- Division of Financial Management
- Legislative Services Office (Budget & Policy Analysis)
- Joint Finance-Appropriations Committee (JFAC)
- State Controller
- Taxpayers and beneficiaries of funded programs

IDAHO BUDGET CONTEXT:
- Idaho requires a balanced budget (constitutional requirement)
- The General Fund is the primary discretionary fund
- JFAC sets agency budgets; the full legislature approves
- Idaho maintains a Budget Stabilization Fund (rainy day fund)
- Personnel costs typically comprise 80%+ of agency budgets
- Idaho's fiscal year runs July 1 - June 30

ANALYSIS FOCUS:
- What is the total appropriation and fund source(s)?
- Is this ongoing or one-time funding?
- How does this compare to the prior year's appropriation?
- What are the major line items and their purposes?
- Does this include new FTEs (positions)?
- Are there prior year maintenance costs built into the base?
- What performance measures or outcomes are expected?
- How does this fit within total General Fund availability?

QUESTIONS TO PROBE:
- Is this appropriation sustainable within projected revenue growth?
- What happens if this program underperforms or costs exceed projections?
- Are there prior year unspent funds being redirected?
- What is the agency's capacity to spend this appropriation effectively?
""",
}


def _extract_subjects(bill_data: Dict[str, Any]) -> list:
    """Extract subject names from bill data."""
    subjects = []
    bill_data = bill_data.get("bill", bill_data) if isinstance(bill_data, dict) else {}
    raw_subjects = bill_data.get("subjects", []) or []

    for subj in raw_subjects:
        if isinstance(subj, dict):
            name = subj.get("subject_name", "").strip()
            if name:
                subjects.append(name)
        elif isinstance(subj, str):
            subjects.append(subj.strip())

    return subjects


def _get_domains_for_subjects(subjects: list) -> list:
    """Map subject names to domain categories."""
    domains = set()
    for subject in subjects:
        # Direct match
        if subject in SUBJECT_TO_DOMAIN:
            domains.add(SUBJECT_TO_DOMAIN[subject])
        else:
            # Partial match - check if subject contains or is contained by any key
            subject_lower = subject.lower()
            for key, domain in SUBJECT_TO_DOMAIN.items():
                if key.lower() in subject_lower or subject_lower in key.lower():
                    domains.add(domain)
                    break
    return list(domains)


def _build_domain_context(subjects: list) -> str:
    """Build domain-specific guidance based on bill subjects."""
    if not subjects:
        return ""

    domains = _get_domains_for_subjects(subjects)
    if not domains:
        # No matching domains - provide generic guidance with the subjects
        return f"""
═══════════════════════════════════════════════════════════════════
BILL SUBJECT AREAS
═══════════════════════════════════════════════════════════════════
This bill is categorized under: {', '.join(subjects)}

Consider the stakeholders, implementation requirements, and fiscal implications
relevant to these policy areas in your analysis.
"""

    # Build guidance from matched domains
    guidance_parts = [f"""
═══════════════════════════════════════════════════════════════════
POLICY DOMAIN CONTEXT
═══════════════════════════════════════════════════════════════════
This bill is categorized under: {', '.join(subjects)}

Apply the following domain-specific analysis guidance:
"""]

    for domain in domains[:3]:  # Limit to top 3 domains to avoid prompt bloat
        if domain in DOMAIN_GUIDANCE:
            guidance_parts.append(DOMAIN_GUIDANCE[domain])

    return "\n".join(guidance_parts)


