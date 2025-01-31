"""Find optimal allocation for Smallbank."""

from algorithm import Operation, Template, TemplateSet, Allocation, is_robust, optimal_alloc, IsolationLevel

# Variables
VAR_X = "X"
VAR_Y = "Y"
VAR_Z = "Z"
VAR_X1 = "X1"
VAR_X2 = "X2"
VAR_Y1 = "Y1"
VAR_Z1 = "Z1"
VAR_Z2 = "Z2"

# Relations
R_ACCOUNT = "Account"
R_SAVINGS = "Savings"
R_CHECKING = "Checking"

# Attributes
A_NAME = "Name"
A_CUSTOMERID = "CustomerID"
A_BALANCE = "Balance"


def create_templates() -> dict[str,TemplateSet]:
    """Create the templates"""
    # Balance
    t_bal = Template("Balance", [
        Operation(VAR_X, R_ACCOUNT,
                  readset=frozenset({A_NAME, A_CUSTOMERID})),
        Operation(VAR_Y, R_SAVINGS,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE})),
        Operation(VAR_Z, R_CHECKING,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}))
    ])

    t_bal_pr1 = Template("Balance_pr1", [
        Operation(VAR_X, R_ACCOUNT,
                  readset=frozenset({A_NAME, A_CUSTOMERID}),
                  writeset=frozenset({A_CUSTOMERID})),
        Operation(VAR_Y, R_SAVINGS,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE})),
        Operation(VAR_Z, R_CHECKING,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}))
    ])

    t_bal_pr2 = Template("Balance_pr2", [
        Operation(VAR_X, R_ACCOUNT,
                  readset=frozenset({A_NAME, A_CUSTOMERID})),
        Operation(VAR_Y, R_SAVINGS,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE})),
        Operation(VAR_Z, R_CHECKING,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}))
    ])

    t_bal_pr3 = Template("Balance_pr3", [
        Operation(VAR_X, R_ACCOUNT,
                  readset=frozenset({A_NAME, A_CUSTOMERID})),
        Operation(VAR_Y, R_SAVINGS,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE})),
        Operation(VAR_Z, R_CHECKING,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE}))
    ])

    t_bal_pr23 = Template("Balance_pr23", [
        Operation(VAR_X, R_ACCOUNT,
                  readset=frozenset({A_NAME, A_CUSTOMERID})),
        Operation(VAR_Y, R_SAVINGS,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE})),
        Operation(VAR_Z, R_CHECKING,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE}))
    ])

    # DepositChecking
    t_dc = Template("DepositChecking", [
        Operation(VAR_X, R_ACCOUNT, 
                  readset=frozenset({A_NAME, A_CUSTOMERID})),
        Operation(VAR_Z, R_CHECKING,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE}))
    ])

    t_dc_pr1 = Template("DepositChecking_pr1", [
        Operation(VAR_X, R_ACCOUNT, 
                  readset=frozenset({A_NAME, A_CUSTOMERID}),
                  writeset=frozenset({A_CUSTOMERID})),
        Operation(VAR_Z, R_CHECKING,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE}))
    ])

    # TrasactSavings
    t_ts = Template("TransactSavings", [
        Operation(VAR_X, R_ACCOUNT,
                  readset=frozenset({A_NAME, A_CUSTOMERID})),
        Operation(VAR_Y, R_SAVINGS,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE}))
    ])

    t_ts_pr1 = Template("TransactSavings_pr1", [
        Operation(VAR_X, R_ACCOUNT,
                  readset=frozenset({A_NAME, A_CUSTOMERID}),
                  writeset=frozenset({A_CUSTOMERID})),
        Operation(VAR_Y, R_SAVINGS,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE}))
    ])

    # Amalgamate
    t_am = Template("Amalgamate", [
        Operation(VAR_X1, R_ACCOUNT,
                  readset=frozenset({A_NAME, A_CUSTOMERID})),
        Operation(VAR_X2, R_ACCOUNT,
                  readset=frozenset({A_NAME, A_CUSTOMERID})),
        Operation(VAR_Y1, R_SAVINGS,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE})),
        Operation(VAR_Z1, R_CHECKING,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE})),
        Operation(VAR_Z2, R_CHECKING,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE}))
    ])

    t_am_pr12 = Template("Amalgamate_pr12", [
        Operation(VAR_X1, R_ACCOUNT,
                  readset=frozenset({A_NAME, A_CUSTOMERID}),
                  writeset=frozenset({A_CUSTOMERID})),
        Operation(VAR_X2, R_ACCOUNT,
                  readset=frozenset({A_NAME, A_CUSTOMERID}),
                  writeset=frozenset({A_CUSTOMERID})),
        Operation(VAR_Y1, R_SAVINGS,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE})),
        Operation(VAR_Z1, R_CHECKING,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE})),
        Operation(VAR_Z2, R_CHECKING,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE}))
    ])

    # WriteCheck
    t_wc = Template("WriteCheck", [
        Operation(VAR_X, R_ACCOUNT,
                  readset=frozenset({A_NAME, A_CUSTOMERID})),
        Operation(VAR_Y, R_SAVINGS,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE})),
        Operation(VAR_Z, R_CHECKING,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE})),
        Operation(VAR_Z, R_CHECKING,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE}))
    ])

    t_wc_pr1 = Template("WriteCheck_pr1", [
        Operation(VAR_X, R_ACCOUNT,
                  readset=frozenset({A_NAME, A_CUSTOMERID}),
                  writeset=frozenset({A_CUSTOMERID})),
        Operation(VAR_Y, R_SAVINGS,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE})),
        Operation(VAR_Z, R_CHECKING,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE})),
        Operation(VAR_Z, R_CHECKING,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE}))
    ])

    t_wc_pr2 = Template("WriteCheck_pr2", [
        Operation(VAR_X, R_ACCOUNT,
                  readset=frozenset({A_NAME, A_CUSTOMERID})),
        Operation(VAR_Y, R_SAVINGS,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE})),
        Operation(VAR_Z, R_CHECKING,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE})),
        Operation(VAR_Z, R_CHECKING,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE}))
    ])

    t_wc_pr3 = Template("WriteCheck_pr3", [
        Operation(VAR_X, R_ACCOUNT,
                  readset=frozenset({A_NAME, A_CUSTOMERID})),
        Operation(VAR_Y, R_SAVINGS,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE})),
        Operation(VAR_Z, R_CHECKING,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE})),
        Operation(VAR_Z, R_CHECKING,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE}))
    ])

    t_wc_pr23 = Template("WriteCheck_pr23", [
        Operation(VAR_X, R_ACCOUNT,
                  readset=frozenset({A_NAME, A_CUSTOMERID})),
        Operation(VAR_Y, R_SAVINGS,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE})),
        Operation(VAR_Z, R_CHECKING,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE})),
        Operation(VAR_Z, R_CHECKING,
                  readset=frozenset({A_CUSTOMERID, A_BALANCE}),
                  writeset=frozenset({A_BALANCE}))
    ])


    # return TemplateSet({t_bal, t_dc, t_ts, t_am, t_wc})
    # return TemplateSet({t_bal, t_dc, t_ts, t_am, t_wc_prom})
    # return TemplateSet({t_bal_pr3, t_dc, t_ts, t_am, t_wc_prom})
    # return TemplateSet({t_dc, t_ts, t_am})

    return {
        "default": TemplateSet({t_bal, t_dc, t_ts, t_am, t_wc}),
        "prom_accounts": TemplateSet({t_bal_pr1, t_dc_pr1, t_ts_pr1, t_am_pr12, t_wc_pr1}),
        "pr_c_0_2": TemplateSet({t_bal, t_dc, t_ts, t_am, t_wc_pr2}),
        "pr_c_0_3": TemplateSet({t_bal, t_dc, t_ts, t_am, t_wc_pr3}),
        "pr_c_0_23": TemplateSet({t_bal, t_dc, t_ts, t_am, t_wc_pr23}),
        "pr_c_2_0": TemplateSet({t_bal_pr2, t_dc, t_ts, t_am, t_wc}),
        "pr_c_3_0": TemplateSet({t_bal_pr3, t_dc, t_ts, t_am, t_wc}),
        "pr_c_23_0": TemplateSet({t_bal_pr23, t_dc, t_ts, t_am, t_wc}),
        "pr_c_2_2": TemplateSet({t_bal_pr2, t_dc, t_ts, t_am, t_wc_pr2}),
        "pr_c_2_3": TemplateSet({t_bal_pr2, t_dc, t_ts, t_am, t_wc_pr3}),
        "pr_c_2_23": TemplateSet({t_bal_pr2, t_dc, t_ts, t_am, t_wc_pr23}),
        "pr_c_3_2": TemplateSet({t_bal_pr3, t_dc, t_ts, t_am, t_wc_pr2}),
        "pr_c_3_3": TemplateSet({t_bal_pr3, t_dc, t_ts, t_am, t_wc_pr3}),
        "pr_c_3_23": TemplateSet({t_bal_pr3, t_dc, t_ts, t_am, t_wc_pr23}),
        "pr_c_23_2": TemplateSet({t_bal_pr23, t_dc, t_ts, t_am, t_wc_pr2}),
        "pr_c_23_3": TemplateSet({t_bal_pr23, t_dc, t_ts, t_am, t_wc_pr3}),
        "pr_c_23_23": TemplateSet({t_bal_pr23, t_dc, t_ts, t_am, t_wc_pr23})
    }


def optimal_allocations():
    """Find optimal allocation for Smallbank."""
    all_template_combis = create_templates()
    for name, templates in all_template_combis.items():
        allocation = optimal_alloc(templates)
        print(f"Allocation for {name}:")
        print(allocation)
        print()

def main():
    """main function"""
    # Identify lowest allocation for each promotion choice
    optimal_allocations()
    print("done!")

    # Example 1: Test robustness for a specific template set and allocation
    all_template_combis = create_templates()
    template_set = all_template_combis["pr_c_3_23"]

    # robust against alloc2, not robust against alloc1
    alloc1 = Allocation(template_set, {t: IsolationLevel.READ_COMMITTED for t in template_set.templates})
    
    alloc2 = Allocation(template_set, {t: IsolationLevel.READ_COMMITTED for t in template_set.templates})

    for t in template_set.templates:
        if t.name[:len("Balance")] == "Balance":
            alloc2.mapping[t] = IsolationLevel.SNAPSHOT_ISOLATION
        
    robust, info = is_robust(template_set, alloc1)
    print(f"Robust against alloc1: {robust}")
    # for k, v in info.items():
    #     print(f"{k}: {v}")

    robust, info = is_robust(template_set, alloc2)
    print(f"Robust against alloc2: {robust}")

    # Example 2: Find optimal allocation for a specific template set
    template_set = all_template_combis["pr_c_2_23"]
    allocation = optimal_alloc(template_set)
    print(f"Optimal allocation: {allocation}")

if __name__ == "__main__":
    main()
