from constants import CLOSE_AT_ZCORE_CROSS, STOP_LOSS, TAKE_PROFIT
from func_utils import format_number
from func_public import get_candles_recent
from func_cointegration import calculate_zscore
from func_private import place_market_order
import pandas as pd
import json
import time
from datetime import datetime
from pprint import pprint

# Manage exits
def manage_trade_exits(client):

    """
        Manage exiting open positions 
        Based upon criteria set in constants
    """

    # Initialize saving output
    save_output = []

    # Opening JSON file
    try:
        open_positions_file = open("bot_agents.json")
        open_positions_dict = json.load(open_positions_file)
    except:
        return "complete"

    # Guard : Exit if no open positions in file
    if len(open_positions_dict) < 1:
        return "complete"
    
    # Load trade logger (csv file)
    try:
        trade_logger = pd.read_csv("trade_logger.csv")
    except:
        trade_logger = {"pair": [],
                        "market": [],
                        "entry_date": [],
                        "position_side": [],
                        "entry_price": [],
                        "position_size": [],
                        "entry_zscore": [],
                        "coint_pvalue": [],
                        "half_life": [],
                        "zero_crossing": [],
                        "exit_date": [],
                        "exit_zscore": [],
                        "exit_signal": [],
                        "total_pnl": []
                        }
        trade_logger = pd.DataFrame(trade_logger)

    # Get all open positions per trading platform (including current Pnl)
    exchange_pos = client.private.get_positions(status="OPEN")
    all_exc_pos = exchange_pos.data["positions"]
    markets_live = []
    unrealized_pnl = []
    realized_pnl = []
    entry_price = []
    entry_date = []
    position_size = []
    position_side = []

    for p in all_exc_pos:
        entry_price.append(float(p["entryPrice"]))
        position_size.append(abs(float(p["size"])))
        position_side.append(p["side"])
        entry_date.append(p["createdAt"])
        markets_live.append(p["market"])
        unrealized_pnl.append(float(p["unrealizedPnl"]))
        realized_pnl.append(float(p["realizedPnl"]))

    dict_markets_pnl = {"market": markets_live,
                        "entry_date": entry_date,
                        "position_side": position_side,
                        "entry_price": entry_price,
                        "position_size": position_size,
                        "unrealized_pnl": unrealized_pnl,
                        "realized_pnl": realized_pnl
                        }
    df_markets_pnl = pd.DataFrame(dict_markets_pnl)
    df_markets_pnl["position_initial_value"] = df_markets_pnl["entry_price"]*df_markets_pnl["position_size"]
    df_markets_pnl["total_pnl"]=df_markets_pnl["unrealized_pnl"]+df_markets_pnl["realized_pnl"]

    # Protect API
    time.sleep(0.5)

    # Check all current positions are in the JSON file
    # Exit positions that are not in the JSON file
    markets_saved = []
    for p in open_positions_dict:
        markets_saved.append(p["market_1"])
        markets_saved.append(p["market_2"])
    for p in all_exc_pos:
        p_market = p["market"]
        if p_market not in markets_saved:
        # Get infos about the position we have to close
            df_p = df_markets_pnl.loc[df_markets_pnl["market"] == p_market]
            position_market_extra = df_p["market"].values.tolist()[0]
            position_side_extra = df_p["position_side"].values.tolist()[0]
            position_size_extra = df_p["position_size"].values.tolist()[0]
            # Determine side 
            side_extra = "SELL"
            if position_side_extra == "SHORT":
                side_extra = "BUY"
        # Get markets for reference of tick size
            markets = client.public.get_markets().data
        # Get last price 
            series_extra = get_candles_recent(client, position_market_extra)
            last_price = float(series_extra[-1])
            accept_price_extra = last_price * 1.05 if side_extra == "BUY" else last_price * 0.95
            tick_size_extra = markets["markets"][position_market_extra]["tickSize"]
            step_size_extra = markets["markets"][position_market_extra]["stepSize"]
            accept_price_extra = format_number(accept_price_extra, tick_size_extra)
            size_extra = format_number(position_size_extra, step_size_extra)

            # Close extra position
            try:

                print(">>> Closing extra position <<<")
                print(f"Closing position for {position_market_extra}")

                close_order_extra = place_market_order(
                    client,
                    market=position_market_extra,
                    side=side_extra,
                    size=size_extra,
                    price=accept_price_extra,
                    reduce_only=True
                )

                # Protect API
                time.sleep(0.5)
            
            except Exception as e:
                print(f"Exit failed for extra position: {position_market_extra}")

    # Check all saved positions match older record
    # Exit trade according to any exit trade rules
    for position in open_positions_dict:

        # Initialize is_close_trigger
        is_close = False

        # Extract position matching information from file - market 1
        position_market_m1 = position["market_1"]
        position_size_m1 = float(position["order_m1_size"])
        position_side_m1 = position["order_m1_side"]

        # Extract position matching information from file - market 2
        position_market_m2 = position["market_2"]
        position_size_m2 = float(position["order_m2_size"])
        position_side_m2 = position["order_m2_side"]

        #Get additional informations about the position
        position_coint_pvalue = position["coint_pvalue"]
        position_half_life = position["half_life"]
        position_zero_crossing = position["zero_crossing"]
        position_zscore = position["z_score"]

        # Protect API
        time.sleep(0.5)

        # Get order info for m1 per exchange
        order_m1 = client.private.get_order_by_id(position["order_id_m1"])
        order_market_m1 = order_m1.data["order"]["market"]
        order_size_m1 = float(order_m1.data["order"]["size"])
        order_side_m1 = order_m1.data["order"]["side"]
       
        # Protect API
        time.sleep(0.5)

        # Get order info for m2 per exchange
        order_m2 = client.private.get_order_by_id(position["order_id_m2"])
        order_market_m2 = order_m2.data["order"]["market"]
        order_size_m2 = float(order_m2.data["order"]["size"])
        order_side_m2 = order_m2.data["order"]["side"]

        # Perform matching checks
        check_m1 = position_market_m1 == order_market_m1 and position_size_m1 == order_size_m1 and position_side_m1 == order_side_m1
        check_m2 = position_market_m2 == order_market_m2 and position_size_m2 == order_size_m2 and position_side_m2 == order_side_m2
        check_live = position_market_m1 in markets_live and position_market_m2 in markets_live

        # Guard : If not all match exit with error
        if not check_m1 or not check_m2 or not check_live:
            print(f"Warning: Not all open positions match exchange records for {position_market_m1} and {position_market_m2}")
            continue

        # Get prices
        series_1 = get_candles_recent(client, position_market_m1)
        time.sleep(0.2)
        series_2 = get_candles_recent(client, position_market_m2)
        time.sleep(0.2)

        # Get markets for reference of tick size
        markets = client.public.get_markets().data

        # Protect API
        time.sleep(0.2)

        # Trigger close based on Z-Score
        if CLOSE_AT_ZCORE_CROSS:
            
            # Initialize z_scores
            hedge_ratio = position["hedge_ratio"]
            z_score_traded = position["z_score"]
            if len(series_1) > 0 and len(series_1) == len(series_2):
                spread = series_1 - (hedge_ratio * series_2)
                z_score_current = calculate_zscore(spread).values.tolist()[-1]
            
            # Determine trigger
            z_score_level_check = abs(z_score_current) >= abs(z_score_traded)
            z_score_cross_check = (z_score_current < 0 and z_score_traded > 0) or (z_score_current > 0 and z_score_traded < 0)

            # Close trade
            if z_score_level_check and z_score_cross_check:

                #Keep exit signal for trade logger
                exit_signal="Zscore"

                # Initiate close trigger
                is_close = True

                #Print closing signal
                print(f"Z-score closing signal triggered for position : {position_market_m1}-{position_market_m2}")
            
        #Trigger close based on Stop loss and Take profit

        ##Compute pair current total pnl
        df_m1 = df_markets_pnl.loc[df_markets_pnl["market"] == order_market_m1]
        df_m1 = df_m1.reset_index(drop=True)
        df_m2 = df_markets_pnl.loc[df_markets_pnl["market"] == order_market_m2]
        df_m2 = df_m2.reset_index(drop=True)
        current_pnl = df_m1["total_pnl"].values.tolist()[0]+df_m2["total_pnl"].values.tolist()[0]
        position_initial_value = df_m1["position_initial_value"].values.tolist()[0]+df_m2["position_initial_value"].values.tolist()[0]

        ##Check if stop loss is triggered
        stop_loss_check = ((position_initial_value+current_pnl)/position_initial_value) <= (1-(STOP_LOSS/100))

        if stop_loss_check and is_close==False:

            #Keep exit signal for trade logger
            exit_signal="SL"

            # Initiate close trigger
            is_close = True

            #Print closing signal
            print(f"Stop loss triggered for position : {position_market_m1}-{position_market_m2}")

        ##Check if take profit is triggered
        take_profit_check = ((position_initial_value+current_pnl)/position_initial_value) >= (1+(TAKE_PROFIT/100))

        if take_profit_check and is_close==False:

            #Keep exit signal for trade logger
            exit_signal="TP"

            # Initiate close trigger
            is_close = True

            #Print closing signal
            print(f"Take profit triggered for position : {position_market_m1}-{position_market_m2}")
        
        ###
        # Add any other close logic you want here
        # Trigger is_close
        ###

        # Close positions if triggered
        if is_close:

            #Temporary file to add to trade logger
            pair = position_market_m1+"--"+position_market_m2
            df_m1.loc[0,"pair"] = pair 
            df_m2.loc[0,"pair"] = pair 
            df_m1.loc[0,"exit_signal"] = exit_signal 
            df_m2.loc[0,"exit_signal"] = exit_signal
            df_m1.loc[0,"entry_zscore"] = position_zscore
            df_m2.loc[0,"entry_zscore"] = position_zscore
            df_m1.loc[0,"coint_pvalue"] = position_coint_pvalue
            df_m2.loc[0,"coint_pvalue"] = position_coint_pvalue
            df_m1.loc[0,"half_life"] = position_half_life
            df_m2.loc[0,"half_life"] = position_half_life
            df_m1.loc[0,"zero_crossing"] = position_zero_crossing
            df_m2.loc[0,"zero_crossing"] = position_zero_crossing
            df_m1.loc[0,"exit_zscore"] = z_score_current
            df_m2.loc[0,"exit_zscore"] = z_score_current

            # Determine side - m1
            side_m1 = "SELL"
            if position_side_m1 == "SELL":
                side_m1 = "BUY"
            
            # Determine side - m2
            side_m2 = "SELL"
            if position_side_m2 == "SELL":
                side_m2 = "BUY"
            
            # Get and format Price
            price_m1 = float(series_1[-1])
            price_m2 = float(series_2[-1])
            accept_price_m1 = price_m1 * 1.05 if side_m1 == "BUY" else price_m1 * 0.95
            accept_price_m2 = price_m2 * 1.05 if side_m2 == "BUY" else price_m2 * 0.95
            tick_size_m1 = markets["markets"][position_market_m1]["tickSize"]
            tick_size_m2 = markets["markets"][position_market_m2]["tickSize"]
            step_size_m1 = markets["markets"][position_market_m1]["stepSize"]
            step_size_m2 = markets["markets"][position_market_m2]["stepSize"]
            accept_price_m1 = format_number(accept_price_m1, tick_size_m1)
            accept_price_m2 = format_number(accept_price_m2, tick_size_m2)
            size_m1 = format_number(position_size_m1, step_size_m1)
            size_m2 = format_number(position_size_m2, step_size_m2)

            # Close positions
            try:

                # Close position for market 1
                print(">>> Closing market 1 <<<")
                print(f"Closing position for {position_market_m1}")

                close_order_m1 = place_market_order(
                    client,
                    market=position_market_m1,
                    side=side_m1,
                    size=size_m1,
                    price=accept_price_m1,
                    reduce_only=True
                )

                print(close_order_m1["order"]["id"])
                print(">>> Closing <<<")

                df_m1.loc[0,"exit_date"]=datetime.now().isoformat()

                # Protect API
                time.sleep(1)

                # Close position for market 2
                print(">>> Closing market 2 <<<")
                print(f"Closing position for {position_market_m2}")

                close_order_m2 = place_market_order(
                    client,
                    market=position_market_m2,
                    side=side_m2,
                    size=size_m2,
                    price=accept_price_m2,
                    reduce_only=True
                )

                print(close_order_m2["order"]["id"])
                print(">>> Closing <<<")

                df_m2.loc[0,"exit_date"]=datetime.now().isoformat()
            
            except Exception as e:
                print(f"Exit failed for {position_market_m1} with {position_market_m2}")
                save_output.append(position) 
            
            #Updating trade logger
            df_temp = pd.concat([df_m1[["pair","market","entry_date","position_side","entry_price","position_size","entry_zscore","coint_pvalue",
                                        "half_life","zero_crossing","exit_date","exit_zscore","exit_signal","total_pnl"]],
                                 df_m2[["pair","market","entry_date","position_side","entry_price","position_size","entry_zscore","coint_pvalue",
                                        "half_life","zero_crossing","exit_date","exit_zscore","exit_signal","total_pnl"]]])
            trade_logger = pd.concat([trade_logger,df_temp])

        # Keep record of items and save
        else:
            save_output.append(position) 
            
    # Save remaining items
    print(f"{len(save_output)} Items remaining. Saving file...")
    with open("bot_agents.json", "w") as f:
        json.dump(save_output, f)

    #Save updated trade logger
    print("Saving trade logger...")
    trade_logger = trade_logger.reset_index(drop=True)
    trade_logger.to_csv("trade_logger.csv",index=False)



            






