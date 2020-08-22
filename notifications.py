import requests
import pandas as pd

def notification(event=None, content=None):
    portfolio_query = """
        SELECT *
        FROM `oval-bot-232220.algorithmic_trader.portfolio`
    """
    pf_data = pd.read_gbq(portfolio_query).sort_values(by='Date').reset_index(drop=True).iloc[-1]

    string_pf = pf_data.to_string()

    requests.post(
        url="https://slack.com/api/chat.postMessage",
        data= {
            "token": "xoxp-1339981061888-1316347188643-1301409843815-8b3df0c291709ba395d579e05a421bb9",
            "channel": "#general",
            "text": string_pf
        }
    )