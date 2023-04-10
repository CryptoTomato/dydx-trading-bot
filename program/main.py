from constants import ABORT_ALL_POSITIONS, FIND_COINTEGRATED, PLACE_TRADES, MANAGE_EXITS
import pandas as pd
from datetime import datetime, timedelta
from func_connections import connect_dydx
from func_private import abort_all_positions
from func_public import construct_market_prices
from func_cointegration import store_cointegration_results
from func_entry_pairs import open_positions
from func_exit_pairs import manage_trade_exits
from func_messaging import send_message


# MAIN FUNCTION
if __name__ == "__main__":

    send_message("Bot launch successful")
    
    # Connect to client
    try:
        print("Connecting to Client")
        client = connect_dydx()
    except Exception as e:
        print("Error connecting to client: ",e)
        send_message(f"Failed to connect to client: {e}")
        exit(1)
    
    # Abort all open positions
    if ABORT_ALL_POSITIONS:
        try:
            print("Closing all positions...")
            close_orders = abort_all_positions(client)
        except Exception as e:
            print("Error closing all positions: ",e)
            send_message(f"Error closing all positions: {e}")
            exit(1)
    
    # Find Cointegrated Pairs

    ##Checking if we have to reset cointegrated pairs
    if FIND_COINTEGRATED==False:
        try:
            time_calc_coint = pd.read_csv("coint_calc_time.csv")
            time_calc_coint = time_calc_coint["date"].values.tolist()[-1]
            time_calc_coint = pd.to_datetime(time_calc_coint ,format='%Y-%m-%d %H:%M:%S.%f')
            current_time = datetime.now()
            delta = current_time-time_calc_coint
            delta = delta.total_seconds()/3600
            ##We reset cointegrated pairs every 12 hours
            if delta>=12:
                FIND_COINTEGRATED = True 
            else:
                FIND_COINTEGRATED = False
        except:
           FIND_COINTEGRATED = True

    if FIND_COINTEGRATED:

        # Construct Market Prices
        try:
            print("Fetching market prices, please allow 3 mins...")
            df_market_prices = construct_market_prices(client)
        except Exception as e:
            print("Error constructing market prices: ", e)
            send_message(f"Error constructing market prices: {e}")
            exit(1)
        
        # Store Cointegrated Pairs
        try:
            print("Storing cointegrated pairs...")
            stores_result = store_cointegration_results(df_market_prices)
            if stores_result != "saved":
                print("Error saving cointegrated pairs")
                exit(1)
        except Exception as e:
            print("Error saving cointegrated pairs: ", e)
            send_message(f"Error saving cointegrated pairs: {e}")
            exit(1)

    # Run as always on
    while True:

        # Place trades for opening positions
        if MANAGE_EXITS:
            try:
                print("Managing exits...")
                manage_trade_exits(client)
            except Exception as e:
                print("Error managing exiting positions: ", e)
                send_message(f"Error managing exiting positions: {e}")
                exit(1)


        # Place trades for opening positions
        if PLACE_TRADES:
            try:
                print("Finding trading opportunities...")
                open_positions(client)
            except Exception as e:
                print("Error trading pairs: ", e)
                send_message(f"Error opening trades: {e}")
                exit(1)