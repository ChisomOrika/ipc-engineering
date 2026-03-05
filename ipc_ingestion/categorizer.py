"""
TRANSACTION CATEGORIZATION SYSTEM v3
=====================================
13 categories, priority-based, context-aware.

CATEGORIES:
  Rider Commission, Supplies & Procurement, Fuel & Diesel, Data & Calls,
  Repairs & Maintenance, Bank Charges, Inward Transfer, Staff Welfare,
  Docs & Compliance, Transport, Utilities, Marketing, Salaries

USAGE:
    from categorizer import categorize_dataframe
    df = categorize_dataframe(df)
"""

import re
import pandas as pd


def _is_actual_salary(text):
    text = str(text).lower()
    if not re.search(r'\bsalar(?:y|ies)\b', text):
        return False
    for pat in [
        r'deduct\w*\s+from\s+.*salary',
        r'from\s+(?:his|her|their|my)\s+salary',
        r'medical\s+support.*salary',
        r'loan\s+.*(?:to\s+be\s+)?deduct\w*.*salary',
        r'bail.*salary', r'easy\s+buy.*salary',
        r'(?:bike|phone)\s+repair.*salary', r'fix\s+.*salary',
    ]:
        if re.search(pat, text):
            return False
    for pat in [
        r'\bsalary\s+(?:payment|balance|advance|part\s+payment|loan|contribution|deduction\s+refund)\b',
        r'\badvance\s+salary\b', r'\bpart\s+salary\b', r'\bsalaries\b',
        r'\w+\s+salary\b', r'(?:^|/)\s*salary\s*(?:$|/)',
    ]:
        if re.search(pat, text):
            return True
    if re.search(r'loan.*salary|salary.*loan', text):
        return bool(re.search(r'\bsalary\s+loan\b', text))
    return True


CATEGORIES = {
    # TIER 0: Bank-level
    'Bank Charges': {
        'patterns': [
            r'electronic.*money.*transfer.*levy',
            r'electronic.*transfer.*money.*levy',
            r'stamp\s*duty',
            r'inward.*nip.*charge',
            r'\bvat\b', r'\bsms\s*charge\b', r'\bcharge\s+from\b',
        ],
        'priority': 0,
    },
    'Inward Transfer': {
        'patterns': [r'inward.*transfer', r'inward.*nip.*transfer'],
        'priority': 1,
        'require_credit': True,
    },

    # TIER 1: Payroll
    'Salaries': {
        'patterns': [],
        'priority': 10,
        'custom_fn': _is_actual_salary,
    },

    # TIER 2: Fuel & energy
    'Fuel & Diesel': {
        'patterns': [
            r'\bfuel\s+allowance\b', r'\bfuel\s+for\b', r'\bfuel\b',
            r'\bdiesel\b', r'\bgenerator\b',
        ],
        'priority': 11,
    },

    # TIER 3: Core operations
    'Rider Commission': {
        'patterns': [
            r'\bdelivery\s+payment\b', r'\bweekly\s+pay(?:ment)?\b',
            r'\bpartner\s*(?:ship)?\s+payment\b', r'\bpatnership\s+payment\b',
            r'\brider\s+commission\b', r'\bexternal\s+rider\b',
            r'\bexternal\s+delivery\b', r'\btotal\s+delivery\b',
            r'\blocation\s*delivery\b', r'\bdelivery\s+fee\b',
            r'\bipc\s+partner\b', r'\bipc\s+payment\b',
            r'\bdelivery\b', r'\bdelivey\b',
            r'\bloan\s+refund\b', r'\bloan\s+recovery\b',
            r'\bcashback\b', r'\bpart\s+payment\b',
            r'\brefund\s+from\s+riders\b', r'\briders?\s+upfront\b',
            r'\bbonus\b', r'\bloan\b', r'\bdebt\b',
            r'\bfirday\s+payment\b',
        ],
        'priority': 13,
    },
    'Supplies & Procurement': {
        'patterns': [
            # Food & ingredients
            r'\bperishable\s+items?\b', r'\bstore\s+items?\b',
            r'\bwb\s+items?\b', r'\bw\s*b\s+items?\b',
            r'\bchicken\b', r'\bbig\s+sam\b', r'\bijora\s+chicken\b',
            r'\bmaterial\s+purchase\b', r'\bbread\b',
            r'\bfries\b', r'\bfrench\s+fries\b',
            r'\bspicy\s+corner\b', r'\bspicy\s+conner\b',
            r'\bpouch\s+pack\b',
            r"\bpapa'?s?\s+grill\b", r'\bbutter\b', r'\basun\b', r'\bsnail\b',
            r'\bspices?\b', r'\bc\.s\b', r'\bwb\b', r'\bww\b',
            r'\bpotatoes?\b', r'\bmilk\b', r'\byam\b', r'\bsugar\b',
            r'\bturkey\b', r'\bmeat\b', r'\bbbq\s+sauce\b',
            r'\bpenne\s+pasta\b', r'\bcity\s*sub\b', r'\bcitysub\b',
            r'\brice\b', r'\bmaggi\b', r'\bpure\s*water\b',
            r'\bikeja\s+items\b', r'\bquadri\s+items\b',
            r'\bABC\b', r'\bbacon\b', r'\bflour\b', r'\bsausage\b', r'\bbeef\b',
            r'\bnylons?\b', r'\bchop\s+chop\b',
            r'\bsoopasta\b', r'\bgrillshack\b', r'\bgrill\s+shack\b',
            r'\bwingsville\b', r'\bwings?\s+bistro\b', r'\bwingbistro\b',
            r'\bwings?\b', r'\bcayenne\b', r'\bpepper\b',
            r'#PO-\d+',
            r'\bsnacks?\b', r'\brefreshments?\b',
            r'\bketchup\b', r'\bsweetcorn\b',
            # Office supplies (merged)
            r'\bprinter\b', r'\bink\b', r'\bnotebook\b', r'\bmouse\s*pad\b',
            r'\boffice\s+extension\b', r'\braincoat\b',
            r'\bscale\b', r'\bplaque\b', r'\baward\b',
            r'\btote\s+bag\b', r'\bsouven?ir\b',
            r'\bladder\b', r'\bstamp\b(?!.*duty)',
            # Cleaning & janitorial (merged)
            r'\bcleaning\b', r'\bjanitorial\b', r'\bcleaner\b',
            r'\bdustbin\b', r'\bwaste\b', r'\blawma\b',
            r'\bfumigat\w*\b', r'\bfecal\b', r'\btruck\s+out\b',
            r'\bshirts?\b', r'\buniform\b', r'\bjersey\b',
            r'\bapparel\b', r'\bt[\s-]*shirts?\b',
            r'\bbranded\b.*\bshirt',
        ],
        'priority': 17,
    },

    # TIER 4: Maintenance
    'Repairs & Maintenance': {
        'patterns': [
            r'\brepairs?\b', r'\bbike\s+repair\b', r'\bmaintenance\b',
            r'\bhelmet\b', r'\bclutch\b', r'\bbattery\b', r'\bthrottle\b',
            r'\btyre\b', r'\bbrake\b', r'\bshoe\s+break\b', r'\bbreak\s*pad\b',
            r'\babsorber\b', r'\bspanner\b', r'\bcarriage\b',
            r'\bvulcanizer\b', r'\bbike\s+purchase\b', r'\bbikes?\b',
            r'\bbus\s+(?:repair|service|aliment)\b',
            r'\bworkmanship\b', r'\bfixing\b', r'\bshocker\b',
            r'\bcabreator\b', r'\bbearing\b', r'\bpipe\b', r'\bplumb\w*\b',
            r'\bconstruction\b', r'\brack\b',
            r'\bweld\w*\b', r'\btube\b', r'\bignition\b',
            r'\btiles?\b', r'\bceiling\b', r'\binstallation\b',
            r'\bcold\s+room\b', r'\binspection\b',
            r'\bengine\s+oil\b', r'\boil\s+change\b', r'\bduty\s+oil\b',
            r'\bbike\s+oil\b', r'\b(?:ac|a\.c)\s+servicing\b',
            r'\bgenerator\s+servicing\b', r'\bservicing\b',
        ],
        'priority': 17,
    },

    # TIER 5: Admin
    'Data & Calls': {
        'patterns': [
            r'\bdata\b', r'\bcall\b', r'\bsubscription\b',
            r'\bsms\b', r'\bairtime\b', r'\binternet\b',
        ],
        'priority': 18,
    },
    'Staff Welfare': {
        'patterns': [
            r'\bbirthday\b', r'\bcelebration\b',
            r'\bhandlers?\s+test', r'\bhandlers?\s+medical',
            r'\bfood\s+handler', r'\btest\s+handler',
            r'\bwelfare\b', r'\bhospitality\b', r'\blodge\b', r'\bmedicine\b',
            r'\bmedical\b', r'\bgift\b', r'\bfood\b',
            r'\bmeals?\b', r'\btreatment\b', r'\bpizza\b', r'\bcake\b',
            r'\bwrapping\b', r'\bhmo\b', r'\bhospital\b',
            r'\bchild\s*birth\s+benefit\b', r'\bdispens\w*\s+water\b',
            r'\bwater\b(?=.*dispens)', r'\bend\s+of\s+year\b',
            r'\bchristmas\s+party\b', r'\bparty\b',
            r'\brent\b', r'\bhouse\s+rent\b',
            r'\btraining\b', r'\bconsulting\b', r'\bcoaching\b',
            r'\bwater\s+analysis\b',
            r'\bmedical\s+support\b',
            r'\bmonday\s+(?:staff\s+)?lunch\b', r'\bstaff\s+(?:monday\s+)?lunch\b',
            r'\blunch\s+for\s+staff\b', r'\bpacks?\s+of\s+.*lunch\b',
            r'\bstaff\s+lunch\b', r'\blunch\b',
        ],
        'priority': 15,
    },
    'Utilities': {
        'patterns': [
            r'\belectrical\b', r'\butility\b', r'\butilities\b',
            r'\bmeter\s+token\b', r'\bmeter\b', r'\bprepaid\b',
            r'\bnepa\b', r'\bnapa\b', r'\belectrician\b',
            r'\bbulbs?\b', r'\bsocket\b', r'\bswitch\b',
            r'\belectricity\b',
            r'\bfire\s+extinguish\w*\b', r'\bextinguish\w*\b',
            r'\bsurveliant\b', r'\bsurveillant\b',
        ],
        'priority': 16,
    },
    'Docs & Compliance': {
        'patterns': [
            r'\bdocuments?\b', r'\bregistration\b', r'\blegal\b',
            r'\bbranding\b', r'\bstickers?\b', r'\bprinting\b',
            r'\blicense\b', r'\bpermit\b',
            r'\broad\s+(?:worthiness|safety)\b', r'\bvio\b',
            r'\baccess\s+code\b', r'\btaskforce\b', r'\brenewal\b',
            r'\bid\s+cards?\b', r'\bsign\s*post\b',
            r'\binsurance\b',
            r'\bsecurity\b', r'\bcctv\b', r'\bguard\b',
        ],
        'priority': 23,
    },
    'Transport': {
        'patterns': [
            r'\btransportation\b', r'\btravel\b', r'\btransport\b',
            r'\bflight\b', r'\bairport\b', r'\bticket\b', r'\blogistics\b',
            r'\bwaybill\b', r'\bcourier\b',
        ],
        'priority': 24,
    },
    'Marketing': {
        'patterns': [
            r'\bphotograph\w*\b', r'\bvideograph\w*\b',
            r'\bimage\s+content\b', r'\bcontent\s+for\b',
            r'\bcontent\s+creation\b', r'\bdj\s+fee\b',
            r'\bsip\s+and\s+paint\b', r'\btournament\b',
        ],
        'priority': 25,
    },
}


def categorize_narration(narration, txn_type='debit'):
    text = str(narration).lower()
    sorted_cats = sorted(CATEGORIES.items(), key=lambda x: x[1]['priority'])
    for cat_name, config in sorted_cats:
        if config.get('require_credit') and txn_type != 'credit':
            continue
        if 'custom_fn' in config:
            if config['custom_fn'](text):
                return cat_name
            continue
        for pattern in config['patterns']:
            if re.search(pattern, text, re.IGNORECASE):
                return cat_name
    parts = text.split('/')
    meaningful = [p.strip() for p in parts if len(p.strip()) > 2]
    if len(meaningful) <= 1 and txn_type == 'credit':
        return 'Inward Transfer'
    return 'Uncategorized'


def categorize_dataframe(df, narration_col='narration', txn_type_col='transactiontype',
                         output_col='category'):
    df = df.copy()
    df[output_col] = df.apply(
        lambda row: categorize_narration(
            row.get(narration_col, ''),
            row.get(txn_type_col, 'debit')
        ), axis=1
    )
    return df


def uncategorized_report(df, narration_col='narration', category_col='category'):
    """Show top uncategorized patterns to help add new keywords."""
    uncat = df[df[category_col] == 'Uncategorized'][narration_col].dropna()
    def extract(n):
        parts = str(n).split('/')
        return parts[1].strip() if len(parts) >= 3 else str(n).strip()[:80]
    return uncat.apply(extract).value_counts().reset_index()


if __name__ == '__main__':
    import sys
    input_file = sys.argv[1] if len(sys.argv) > 1 else 'test.xlsx'
    df = pd.read_excel(input_file)
    df = categorize_dataframe(df)
    if 'initiatedat' in df.columns:
        df = df.sort_values(by='initiatedat', ascending=False)
    counts = df['category'].value_counts()
    print(f"\n{'Category':<28} {'Count':>6}  {'%':>6}")
    print("-" * 45)
    for cat, count in counts.items():
        print(f"  {cat:<26} {count:>6}  ({count/len(df)*100:>5.1f}%)")
    print(f"\n  {'TOTAL':<26} {len(df):>6}")
    output_file = input_file.replace('.xlsx', '_categorized.xlsx')
    df.to_excel(output_file, index=False)
    print(f"\nSaved: {output_file}")
