#!/usr/bin/env python3

"""
Monitors Permissioning DB for Node performance,
calculates point value, and reports the pointing to substrate
"""

import argparse
import datetime
import json
import logging as log
import psycopg2
import sys
import os
from substrateinterface import SubstrateInterface, Keypair
import time

#############
# Variables #
#############

substrate_conn_freq = 10  # Frequency of attempting reconnects in seconds
xxdot_url = ""
ns_in_s = float(1e+9)  # Conversion from nanosecond to second
ms_in_ns = 1e+6
THOUSAND = 1_000
state_key = "consensus_points_timestamp"  # Static key used for states table
positive_points_func = 'submit_cmix_points'
negative_points_func = 'submit_cmix_deductions'
raw_points_log = ''


#################
# Main Function #
#################

def main():
    global xxdot_url

    # Process input variables and program arguments
    args = get_args()
    log.info("Running with configuration: {}".format(args))
    xxdot_url = args['xxdot_url']
    wallet_path = os.path.expanduser(args['wallet_path'])
    db_host = args['host']
    db_port = args['port']
    db_name = args['db']
    db_user = args['user']
    db_pass = args['pass']

    # Read Mnemonic from file and create keypair
    with open(wallet_path) as file:
        mnemonic = file.readline()
    keypair = Keypair.create_from_mnemonic(mnemonic.strip())

    conn, substrate = None, None
    try:
        # Define connections
        conn = get_conn(db_host, db_port, db_name, db_user, db_pass)
        substrate = check_chain_connection()

        # Get initial information from blockchain
        point_info = poll_point_info(substrate)

        # Get last timestamp the script has already processed
        last_checked_ts = get_last_checked_timestamp(conn)
        if last_checked_ts is None:
            # If the script hasn't run before, start at epoch
            last_checked_ts = 0

        # Handle all rounds since the script last ran
        current_time = time.time_ns()
        process_period(conn, substrate, keypair, point_info, last_checked_ts, current_time)

        # Initiate the main loop
        last_checked_ts = current_time + 1  # Increment to prevent period overlap
        while True:
            try:
                # Get up-to-date period information from blockchain
                point_info = poll_point_info(substrate)
                period = int(point_info['period'] * ms_in_ns)

                # Wait until the next period arrives
                next_period = last_checked_ts + period
                log.info(f"Period: {period}, Start: {last_checked_ts}, End: {next_period}")
                wait_time = float(next_period - time.time_ns()) / ns_in_s
                if wait_time > 0:
                    time.sleep(wait_time)

                # Process the period
                process_period(conn, substrate, keypair, point_info, last_checked_ts, next_period)
                last_checked_ts = next_period + 1  # Increment to prevent period overlap
            except ConnectionError as e:
                log.warning(f"Connection error occurred, attempting reconnect: {e}")
                substrate.close()
                substrate = check_chain_connection()

    except Exception as e:
        log.fatal(f"Unhandled exception occurred: {e}", exc_info=True)
        if conn:
            conn.close()
        if substrate:
            substrate.close()
        sys.exit(1)


#######################
# Auxiliary Functions #
#######################

def process_period(conn, substrate, keypair, point_info, start_period, end_period):
    """
    Get round info for the specified period, calculate associated points,
    and push the resulting point information to blockchain

    :param conn: Database conn object
    :param substrate: Substrate conn object
    :param keypair: Substrate keypair file
    :param point_info: Point mapping from poll_point_info
    :param start_period: Timestamp to begin pointing rounds from
    :param end_period: Timestamp to end pointing rounds at
    :return: None
    """
    # Get round information for the given period
    round_info, active_nodes = get_round_info_for_period(conn, start_period, end_period)

    # Calculate points for the retrieved round information
    wallet_points, raw_points = round_point_computation(point_info, round_info, active_nodes)

    # Define Lists
    positive = []
    negative = []

    # Calculate Performance Results
    for key, value in wallet_points.items():
        if value > 0:
            positive.append([key, value])
        elif value < 0:
            negative.append([key, abs(value)])

    # Push pointing to blockchain
    try:
        if len(positive) > 0:
            push_point_info(substrate, positive_points_func, keypair, positive)
        if len(negative) > 0:
            push_point_info(substrate, negative_points_func, keypair, negative)
    except Exception as e:
        log.error(f"Failed to push point info: {e}")
        raise e

    with open(raw_points_log, "a") as f:
        f.write(f"[{datetime.datetime.now()}] {raw_points}\n")

    # Save end_period timestamp to database to lock in the operation
    update_last_checked_timestamp(conn, end_period)


def round_point_computation(point_info, round_info, active_nodes):
    """
    round_point_computation performs the main calculations for this script
    :param round_info:
    :param active_nodes:
    :param point_info: point_info dictionary polled from consensus
    :return: wallet_points, raw_points dicts
    """
    bin_multipliers = point_info['multipliers']
    success_points = point_info['success_points']
    fail_points = point_info['failure_points']
    country_bins = point_info['countries']

    wallet_points = {}  # dictionary of wallet -> points to pass to push_point_info
    raw_points_dict = {} # Dict of raw points (without multipliers) to print to a log file
    node_multipliers = {}  # Dictionary containing point multipliers for each node
    node_wallets = {}  # Dictionary parsed from active nodes to more efficiently associate ID with Wallet ID

    # Parse active nodes into dictionaries
    for row in active_nodes:
        wallet_address = row[0]
        node_id = bytes(row[1])
        node_country = row[2]  # Database query for country by node id
        node_bin = country_bins[node_country]  # Get bin associated with country
        node_multipliers[node_id] = bin_multipliers[node_bin]  # Assign multiplier to node
        node_wallets[node_id] = wallet_address  # Add wallet association for node id
        wallet_points[wallet_address] = 0
        raw_points_dict[wallet_address] = 0

    # Calculate point information for each round
    for row in round_info:
        # precomp_start = row[1]
        precomp_end = row[2]
        # rt_start = row[3]
        # rt_end = row[4]
        round_err = row[5]
        topology = row[6]

        log.debug(f"Topology: {topology}, round_err: {round_err}")
        if round_err:
            # Determine negative points for failures
            round_id = row[0]
            if precomp_end == datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.timezone.utc):
                log.debug(f"Round {round_id}: Precomp error")
            else:
                log.debug(f"Round {round_id}: Realtime error")
                for node_id in topology:
                    # NOTE: Weirdness can result here from nodes going offline between eras. Should be reviewed.
                    wallet = node_wallets.get(bytes(node_id))
                    if wallet:
                        wallet_points[wallet] += fail_points
                        raw_points_dict[wallet] += fail_points
        else:
            # Handle point multipliers
            # NOTE: Weirdness can result here from nodes going offline between eras. Should be reviewed.
            multipliers = [node_multipliers[bytes(node_id)] for node_id in topology
                           if node_multipliers.get(bytes(node_id))]
            max_multiplier = max(multipliers)

            # Assign points to wallets
            for node_id in topology:
                mult = node_multipliers.get(bytes(node_id))
                if mult:
                    node_mult = (max_multiplier + mult) / 2
                    log.debug(f"node multiplier: {node_mult}")
                    points = success_points * node_mult

                    # NOTE: Weirdness can result here from nodes going offline between eras. Should be reviewed.
                    wallet = node_wallets.get(bytes(node_id))
                    if wallet:
                        wallet_points[wallet] += points
                        raw_points_dict[wallet] += success_points
                    else:
                        log.warning(f"no wallet found for nid {bytes(node_id)}")
                else:
                    log.warning(f"no mult found for nid {bytes(node_id)}")

    log.debug(f"Wallet points: {wallet_points}")
    return wallet_points, raw_points_dict


#######################
# Database Functions #
#######################

def update_last_checked_timestamp(conn, t1):
    """
    Update last checked timestamp for this script in the database
    :param conn: database connection object
    :param t1: t1 timestamp from loop
    :return:
    """
    cur = conn.cursor()
    update_command = "INSERT INTO states (key, value) VALUES (%s, %s) " \
                     "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;"
    try:
        cur.execute(update_command, (state_key, str(t1),))
        log.debug(cur.query)
        conn.commit()
    except Exception as e:
        log.error(f"Failed to update last checked timestamp: {cur.query}")
        raise e
    finally:
        cur.close()


def get_last_checked_timestamp(conn):
    """
    Get last checked timestamp from the database
    :param conn: database connection object
    :return: int(t1) or None
    """
    cur = conn.cursor()
    select_command = "SELECT value FROM states WHERE key = %s;"
    try:
        cur.execute(select_command, (state_key,))
        log.debug(cur.query)
    except Exception as e:
        log.error(f"Failed to get last checked timestamp from db: {cur.query}")
        cur.close()
        raise e

    res = cur.fetchone()
    cur.close()
    if res is None:
        log.info("No consensus point timestamp found!")
        return None
    return int(res[0])


def get_round_info_for_period(conn, t1, t2):
    """
    Get rounds for a given interval
    returns 2d list of round info, form
    [[rid, precomp start, precomp end, realtime start, realtime end, error, topology], ...]
    :param conn: database connection object
    :param t1: start timestamp in nanoseconds
    :param t2: end timestamp in nanoseconds
    :return: list[round_info], list[active_nodes]
    """
    cur = conn.cursor()

    # Execute active nodes query
    active_nodes_query = "SELECT wallet_address, active_nodes.id, nodes.sequence FROM active_nodes \
            INNER JOIN nodes ON nodes.id = active_nodes.id;"
    try:
        cur.execute(active_nodes_query)
        log.debug(cur.query)
    except Exception as e:
        log.error(f"Failed to get active nodes from DB: {cur.query}")
        cur.close()
        raise e
    active_nodes = cur.fetchall()

    # Convert timestamps to DB-readable format
    t1_str = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(float(t1) / ns_in_s))
    t2_str = time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(float(t2) / ns_in_s))

    # Execute round info query
    round_info_query = f"SELECT id, precomp_start, precomp_end, realtime_start, realtime_end, \
            EXISTS (select * from round_errors where round_metric_id = round_metrics.id) as err, array_agg(node_id) \
            FROM round_metrics inner join topologies on round_metrics.id = topologies.round_metric_id \
            WHERE precomp_start >= '{t1_str}+00' AND precomp_start <= '{t2_str}+00' group by round_metrics.id"
    log.debug(round_info_query)
    try:
        cur.execute(round_info_query)
    except Exception as e:
        log.error(f"Failed to get round info from DB: {cur.query}")
        cur.close()
        raise e
    rounds = cur.fetchall()

    cur.close()
    return rounds, active_nodes


#######################
# Consensus Functions #
#######################

def poll_point_info(substrate):
    """
    Polling Substrate Chain information to calculate era points
    :param substrate: Substrate chain connection object
    :return: dictionary of {
        “period”: int,
        “success_points”: int,
        “failure_points”: int,
        “countries”: {
            CountryCode(string): Bin(int)
        },
        “multipliers”: {
            Bin(int): Multiplier(float)
        }
    }
    """
    # Get Cmix Variables
    try:
        cmix_variables = substrate.query(
            module='XXCmix',
            storage_function='CmixVariables',
            params=[]
        )
    except Exception as e:
        log.error("Failed to poll_point_info")
        raise e

    # Dict: Country -> Bin
    points_info = {'period': cmix_variables.value['performance']['period'],
                   'success_points': cmix_variables.value['performance']['points']['success'],
                   'failure_points': -cmix_variables.value['performance']['points']['failure'],
                   'countries': parse_bins(cmix_variables.value['performance']['countries']), 'multipliers': {}}

    # Dict: Bin -> Multipliers
    bins_list = cmix_variables.value['performance']['multipliers']
    for elem in bins_list:
        points_info['multipliers'][elem[0]] = (elem[1] / THOUSAND)

    log.debug(f"Obtained point info: {points_info}")
    return points_info


def push_point_info(substrate, call_function, keypair, wallet_points):
    """
    Push positive / negative array of points per wallet through an Extrinsic to Substrate Chain
    :param keypair: Substrate keypair object for main wallet
    :param call_function: Substrate function to call
    :param substrate: Substrate chain connection object
    :param wallet_points: List of list of wallet -> point pairs
    """
    log.debug(f"Attempting to submit {call_function} with points: {wallet_points}")

    call = substrate.compose_call(
        call_module='XXCmix',
        call_function=call_function,
        call_params={"data": wallet_points}
    )
    extrinsic = substrate.create_signed_extrinsic(call=call, keypair=keypair)
    try:
        receipt = substrate.submit_extrinsic(extrinsic, wait_for_inclusion=True)
    except Exception as e:
        log.error(f"Failed to push_point_info: {extrinsic}")
        raise e

    log.debug("[Extrinsic] {}('{}') sent and included in block '{}'".format(
        call_function, receipt.extrinsic_hash, receipt.block_hash))


########################
# Basic Functions      #
########################

def get_args():
    """
    get_args controls the argparse usage for the script.  It sets up and parses
    arguments and returns them in dict format
    """
    global raw_points_log
    parser = argparse.ArgumentParser(description="Options for point assignment script")
    parser.add_argument("--verbose", action="store_true",
                        help="Print debug logs", default=False)
    parser.add_argument("--log", type=str,
                        help="Path to output log information",
                        default="/tmp/points.log")
    parser.add_argument("--raw-points-log", type=str,
                        help="Path to output log information",
                        default="/cmix/raw-points.log")
    parser.add_argument("--xxdot-url", type=str, help="xxdot url",
                        default="ws://localhost:9944")
    parser.add_argument("--wallet-path", type=str,
                        help="Wallet key path for pushing point info",
                        default="wallet.key")
    parser.add_argument("-a", "--host", metavar="host", type=str,
                        help="Database server host for attempted connection",
                        default="localhost")
    parser.add_argument("-p", "--port", type=int,
                        help="Port for database connection",
                        default=5432)
    parser.add_argument("-d", "--db", type=str,
                        help="Database name",
                        default="cmix_server")
    parser.add_argument("-U", "--user",  type=str,
                        help="Username for connecting to database",
                        default="cmix")
    parser.add_argument("--pass", type=str,
                        help="DB password")

    args = vars(parser.parse_args())
    log.basicConfig(format='[%(levelname)s] %(asctime)s: %(message)s',
                    level=log.DEBUG if args['verbose'] else log.INFO,
                    datefmt='%d-%b-%y %H:%M:%S',
                    filename=args["log"])
    raw_points_log = args['raw_points_log']
    return args


def json_to_dict(filename):
    """
    Read data from json file to dict structure
    :param filename: json filename
    :return: dict with json file data
    """
    with open(filename) as json_file:
        return json.load(json_file)


def parse_bins(countries_list):
    """
    Parse and cast list of (country code, bin)
    :param countries_list: list of [country code(bytes), bin]
    :return: dict of Country Code (string) -> Bin (int)
    """
    ret = {}
    for elem in countries_list:
        country = bytes.fromhex(elem[0][2:]).decode('utf-8')
        ret[country] = (elem[1])
    return ret


########################
# Connection Functions #
########################

def get_conn(host, port, db, user, pw):
    """
    Create a database connection object for use in the rest of the script
    :param host: Hostname for database connection
    :param port: port for database connection
    :param db: database name
    :param user: database user
    :param pw: database password
    :return: connection object for the database
    """
    conn_str = "dbname={} user={} password={} host={} port={}".format(db, user, pw, host, port)
    try:
        conn = psycopg2.connect(conn_str)
    except Exception as e:
        log.error(f"Failed to get database connection: {conn_str}")
        raise e
    log.info("Connected to {}@{}:{}/{}".format(user, host, port, db))
    return conn


def check_chain_connection():
    """
    Check Blockchain connection every <conn_freq> seconds until it returns a provider
    :return: Substrate Network Provider used to query blockchain
    """
    substrate = get_substrate_provider()
    while substrate is None:
        time.sleep(substrate_conn_freq)
        log.info(f"Attempting to connect to substrate...")
        substrate = get_substrate_provider()
    return substrate


def get_substrate_provider():
    """
    Get Substrate Provider listening on websocket of the Substrate Node configured with network registry from server
    :return: Substrate Network Provider used to query blockchain
    """
    try:
        return SubstrateInterface(url=xxdot_url,)
    except ConnectionRefusedError:
        log.warning("No local Substrate node running.")
        return None
    except Exception as e:
        log.error(f"Failed to get substrate chain connection: {e}")
        raise e


#########
# ENTER #
#########

if __name__ == "__main__":
    main()
