import pandas as pd

def backup_database(event=None, context=None):
    # models currently running needing to be backed up
    # get live portfolio
    portfolio_query = """
        SELECT *
        FROM `oval-bot-232220.algorithmic_trader.portfolio`
    """
    pf_history = pd.read_gbq(portfolio_query).sort_values(by='Date').reset_index(drop=True)

    # get backup portfolio
    backup_query = """
        SELECT *
        FROM `oval-bot-232220.algorithmic_trader.backup`
    """
    backup_data = pd.read_gbq(backup_query).sort_values(by='Date').reset_index(drop=True)

    # add new portfolio to existing portfolio data and push to gbq
    pf_data = backup_data.append(pf_history.iloc[-1], ignore_index=True)
    pf_data.to_gbq(destination_table='algorithmic_trader.backup',
            project_id="oval-bot-232220",
            if_exists='replace')

backup_database()