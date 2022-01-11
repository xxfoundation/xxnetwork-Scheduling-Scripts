#!/usr/bin/env python3

"""
Monitors substrate for Node and Network parameter information,
inserts Node information into authorizer DB,
and inserts Node information and Network parameters into Permissioning DB
"""
import subprocess
from collections import namedtuple
import copy
import json
import argparse
import logging as log
import psycopg2
import time
import sys
from substrateinterface import SubstrateInterface

#############
# Variables #
#############

# Frequency Event Definition
polling_freq = 30  # seconds
conn_freq = 10  # Frequency of attempting reconnects in seconds
xxdot_url = ""
blurb_prefix = 'Blurb'

# Key mappings
Identity = namedtuple('Identity', 'cmix_id name email url twitter blurb')
NodeInfoRow = namedtuple('NodeInfo', 'node_id app_id name email url twitter blurb')
ChainConf = namedtuple('ChainConf', 'timeouts_precomputation timeouts_realtime timeouts_advertisement '
                                    'scheduling_team_size scheduling_batch_size scheduling_min_delay '
                                    'scheduling_pool_threshold registration_max registration_period')


#################
# Main Function #
#################

def main():
    global xxdot_url

    # Process input variables and program arguments
    args = get_args()
    log.info("Running with configuration: {}".format(args))
    db_host = args['host']
    auth_db_host = args['authdb']
    db_port = args['port']
    db_name = args['db']
    db_user = args['user']
    db_pass = args['pass']
    xxdot_url = args['xxdot_url']

    conn, auth_conn, substrate = None, None, None
    try:
        # Define connections
        conn = get_conn(db_host, db_port, db_name, db_user, db_pass)
        auth_conn = get_conn(auth_db_host, db_port, db_name, db_user, db_pass)
        substrate = check_chain_connection()

        # Wait for database creation
        log.info("Waiting for table creation...")
        table_exists = check_table(conn, "active_nodes")
        while not table_exists:
            time.sleep(conn_freq)
            table_exists = check_table(conn, "active_nodes")

        # Main loop
        active_dict = dict()
        auth_nids = set()
        current_bins = []
        current_chain_conf = {}
        init_auth = get_authorizer_nodes(auth_conn)
        for i in init_auth:
            auth_nids.add(bytes(i[0]))
        while True:
            try:
                log.info("Polling substrate...")

                # Deal with bins
                bins, chain_conf = poll_cmix_info(substrate)
                log.debug(f"Polled {len(bins)} bins!")
                if bins != current_bins:
                    log.info(f"Updating GeoBins: {bins}")
                    set_bins(conn, bins)
                    current_bins = bins

                log.debug(f"Polled configuration: {chain_conf}")
                if current_chain_conf != chain_conf._asdict():
                    log.info(f"Updating configuration options: {chain_conf}")
                    update_config_options(conn, chain_conf)
                    current_chain_conf = chain_conf._asdict()

                # Deal with active nodes
                new_dict = poll_active_nodes(substrate)
                log.debug(f"Polled {len(new_dict)} active nodes!")
                if active_dict != new_dict:
                    log.info(f"Updating active nodes: {len(new_dict)} nodes")

                    # Extract node IDs for authorizer (cmix_id with node type byte added)
                    new_auth_nids = [i.cmix_id + b'\x02' for i in new_dict.values()]
                    new_auth_set = set(new_auth_nids)
                    to_add = new_auth_set.difference(auth_nids)
                    to_delete = auth_nids.difference(new_auth_set)
                    to_revoke = set_authorizer_nodes(auth_conn, to_add, to_delete)
                    auth_nids = new_auth_set
                    revoke_auth(to_revoke)

                    # Pass a copy because the dict will be mutated
                    set_active_nodes(conn, copy.deepcopy(new_dict))

                    #
                    active_dict = new_dict

                time.sleep(polling_freq)
            except ConnectionError as e:
                log.warning(f"Connection error occurred, attempting reconnect: {e}")
                substrate.close()
                substrate = check_chain_connection()

    except Exception as e:
        log.fatal(f"Unhandled exception occurred: {e}", exc_info=True)
        if conn:
            conn.close()
        if auth_conn:
            auth_conn.close()
        if substrate:
            substrate.close()
        sys.exit(1)


######################
# Basic Functions    #
######################

def get_args():
    """
    get_args controls the argparse usage for the script.  It sets up and parses
    arguments, passes them to validate, then returns them in dict format
    """
    parser = argparse.ArgumentParser(description="Handle opts for scheduling script")
    # database connection info arguments
    parser.add_argument("-a", "--host", type=str, help="Database server host for attempted connection",
                        default="localhost")
    parser.add_argument("-p", "--port", type=int, help="Port for database connection", default=5432)
    parser.add_argument("-d", "--db", type=str, help="Database name", default="cmix_server")
    parser.add_argument("-U", "--user", type=str, help="Username for connecting to database",
                        default="cmix")
    parser.add_argument("--pass", type=str, help="DB password")
    parser.add_argument("--authdb", type=str, help="Auth db address")
    # script control input
    parser.add_argument("--verbose", action="store_true",
                        help="Print debug logs",
                        default=False)
    parser.add_argument("--log", type=str,
                        help="Path to output log information",
                        default="/tmp/scheduling.log")
    # Constant Variables
    parser.add_argument("--xxdot-url", type=str, help="xxdot url", default="ws://localhost:9944")

    args = vars(parser.parse_args())
    log.basicConfig(format='[%(levelname)s] %(asctime)s: %(message)s',
                    level=log.DEBUG if args['verbose'] else log.INFO,
                    datefmt='%d-%b-%y %H:%M:%S',
                    filename=args["log"])
    return args


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
        time.sleep(conn_freq)
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


#######################
# Auxiliary Functions #
#######################

def revoke_auth(to_revoke):
    """
    revoke_auth accepts a list of node IP addresses to revoke auth from
    :param to_revoke: list of node IP addresses
    """
    log.info(f"Revoking access to {len(to_revoke)} nodes...")
    for node_ip in to_revoke:
        cmd = f"sudo nft -a list chain inet filter input | grep '{node_ip}' | awk -F'handle ' '{{print $2}}' | xargs -Ixxx sudo nft delete rule inet filter input handle xxx"
        log.debug(cmd)
        p = subprocess.Popen(cmd.split())
        output, error = p.communicate()
        if output:
            log.debug(output)
        if error:
            raise IOError(error)


def id_to_reg_code(cmix_id):
    """
    Helper to convert cmix ID to reg code
    :param bytes cmix_id: bytes cmix ID in bytes
    :return string: reg code string
    """
    return f"0x{cmix_id.hex()}"


def json_to_dict(filename):
    """
    Read data from json file to dict structure
    :param string filename: json filename
    :return dict: json file data
    """
    with open(filename) as json_file:
        return json.load(json_file)


def parse_bins(countries_list):
    """
    Parse and cast list of (country code, bin)
    :param list[bytes, str] countries_list: list of [country code(bytes), bin]
    :return dict: Country Code (string) -> Bin (int)
    """
    ret = {}
    for elem in countries_list:
        country = bytes.fromhex(elem[0][2:]).decode('utf-8')
        ret[country] = (elem[1])
    return ret


#######################
# Consensus Functions #
#######################

def poll_cmix_info(substrate):
    """
    Polling Substrate Chain binning list
    :return map, ChainConf: map of Country Code (string) -> Bin (int)
    """

    # Get Cmix Variables
    try:
        cmix_variables = substrate.query(
            module='XXCmix',
            storage_function='CmixVariables',
            params=[]
        )
    except Exception as e:
        log.error("Failed to poll_bin_info")
        raise e

    # Dict: Country -> Bin
    bin_data = parse_bins(cmix_variables.value['performance']['countries'])

    precomp_timeout = 0
    rt_timeout = 0
    advertisment_timeout = 0
    sched_teamsize = 0
    sched_batchsize = 0
    sched_mindelay = 0
    sched_pool = 0
    reg_max = 0
    reg_period = 0

    to = cmix_variables.value['timeouts']
    if to:
        precomp_timeout = to['precomputation']
        rt_timeout = to['realtime']
        advertisment_timeout=to['advertisement']

    sched = cmix_variables.value['scheduling']
    if sched:
        sched_teamsize = sched['team_size']
        sched_batchsize = sched['batch_size']
        sched_mindelay = sched['min_delay']
        permill = 1000000
        sched_pool = sched['pool_threshold'] / permill

    reg = cmix_variables.value['registration']
    if reg:
        reg_max = reg['max']
        reg_period = reg['period']

    return bin_data, ChainConf(timeouts_precomputation=precomp_timeout, timeouts_realtime=rt_timeout,
                               timeouts_advertisement=advertisment_timeout, scheduling_team_size=sched_teamsize,
                               scheduling_batch_size=sched_batchsize, scheduling_min_delay=sched_mindelay,
                               scheduling_pool_threshold=sched_pool, registration_max=reg_max,
                               registration_period=reg_period)


def poll_active_nodes(substrate):
    """
    Polling Substrate Chain information to feed cMix database with Active Nodes
    identity dict contains:
            "cmix_id" (bytes)
            "name" (string) (wallet name)
            "email" (string)
            "url" (string)
            "twitter" (string)
    :return: dict of Validator Wallet Address (string) -> Identity
    """
    try:
        validator_set = substrate.query("Session", "Validators")
    except Exception as e:
        log.error(f"Failed to query validators: {e}")
        raise e

    try:
        offending_set = substrate.query("Staking", "OffendingValidators")
    except Exception as e:
        log.error(f"Failed to query offending validators: {e}")
        raise e

    # Bc we use pop to remove disabled, go backwards through this list. Otherwise, popping early index shifts later ones
    offending_set.value.reverse()
    for val in offending_set.value:
        try:
            validator_set.value.pop(val[0])
        except IndexError as e:
            log.error(f"Invalid offending set value {val} for validator set of {len(validator_set.value)}: {e}")

    ids_map = {}
    for val in validator_set.value:
        try:
            data = substrate.query("Staking", "Bonded", [val])
        except Exception as e:
            log.error(f"Failed to query Staking Bonded: {val}")
            raise e
        controller = data.value

        try:
            data = substrate.query("Staking", "Ledger", [controller])
        except Exception as e:
            log.error(f"Failed to query Staking Ledger: {controller}")
            raise e
        ledger = data.value

        cmix_id = ledger['cmix_id']
        cmix_id = bytes.fromhex(cmix_id[2:])  # convert string to bytes

        try:
            data = substrate.query(
                module='Identity',
                storage_function='IdentityOf',
                params=[val]
            )
        except Exception as e:
            log.error(f"Failed to query Identity {val}")
            raise e

        name = ""
        email = ""
        url = ""
        twitter = ""
        blurb = ""
        if data.value and "info" in data.value:
            identity_info = data.value["info"]

            # Internal function to extract info if it exists
            # Rust interface gives us dictionaries w/ keys by type
            # so we want any data of 'Raw' type for given identifier
            def get_raw(struct, identifier):
                return struct[identifier]['Raw'] if 'Raw' in struct[identifier] else ""

            # Set any fields returned in identity info
            name = get_raw(identity_info, "display")  # Should this use display or legal
            email = get_raw(identity_info, "email")
            url = get_raw(identity_info, "web")
            twitter = get_raw(identity_info, "twitter")

            #  Loop through additional fields - currently, only blurb is here
            if "additional" in identity_info:
                for i in identity_info["additional"]:
                    field = get_raw(i, 0)
                    if field.startswith(blurb_prefix):
                        blurb = blurb + field[6:]
                        blurb = blurb + get_raw(i, 1)

        identity = Identity(cmix_id=cmix_id, name=name, email=email, url=url, twitter=twitter, blurb=blurb)
        ids_map[val] = identity

    return ids_map


######################
# Database Functions #
######################

def update_config_options(conn, chain_conf):
    """
    update config based on chain data
    :param conn:
    :param ChainConf chain_conf:
    :return:
    """
    update_state_command = "INSERT INTO states (key, value) VALUES (%s, %s) " \
                           "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;"
    cur = conn.cursor()
    for key, value in chain_conf._asdict().items():
        if float(value) <= 0:
            log.warning(f"Value for {key} not set in chain, skipping update")
            continue
        try:
            cur.execute(update_state_command, (key, str(value),))
            conn.commit()
        except Exception as e:
            log.error(f"Failed to update state: {cur.query}")
            cur.close()
            raise e
    cur.close()


def get_node_info(conn, code):
    """
    :param psycopg2.connection conn: Connection object for db
    :param str code: Registration code
    :return NodeInfoRow:
    """
    cur = conn.cursor()
    get_node_info_command = "SELECT nodes.id as nid, applications.id as appid, name, url, email, twitter, blurb FROM " \
                            "nodes INNER JOIN applications ON nodes.application_id = applications.id WHERE code = %s;"
    try:
        cur.execute(get_node_info_command, (code,))
        log.debug(cur.query)
    except Exception as e:
        log.error(f"Failed to get node info: {cur.query}")
        cur.close()
        raise e

    row = cur.fetchone()
    cur.close()
    return NodeInfoRow(node_id=row[0], app_id=row[1], name=row[2], url=row[3],
                       email=row[4], twitter=row[5], blurb=row[6]) if row else None


def update_application_if_needed(conn, identity, node_info_row):
    """
    Compares NodeInfoRow to Identity information & updates applications table if necessary
    :param conn:
    :param Identity identity:
    :param NodeInfoRow node_info_row:
    :return:
    """
    if identity.name == node_info_row.name and identity.url == node_info_row.url and \
            identity.blurb == node_info_row.blurb and identity.email == node_info_row.email and \
            identity.twitter == node_info_row.twitter:
        return  # If nothing needs to change, exit

    cur = conn.cursor()
    update_command = "UPDATE applications SET name = %s, url = %s, blurb = %s, email = %s, twitter = %s WHERE id = %s;"
    try:
        cur.execute(update_command, (identity.name, identity.url, identity.blurb, identity.email,
                                     identity.twitter, node_info_row.app_id))
        log.debug(cur.query)
    except Exception as e:
        log.error(f"Failed to update applications entry: {cur.query}")
        raise e
    finally:
        cur.close()


def set_bins(conn, bins):
    """
    set_bins sets bins in the scheduling database
    :param conn:
    :param bins:
    :return:
    """
    cur = conn.cursor()

    # Build the insert command
    truncate_command = "TRUNCATE geo_bins;"
    insert_command = "INSERT INTO geo_bins (country, bin) VALUES " + (' (%s, %s),' * len(bins))
    insert_command = insert_command[:-1] + " ON CONFLICT DO NOTHING;"
    insert_list = []
    for k, v in bins.items():
        insert_list = insert_list + [k, v]

    # Execute the queries
    try:
        cur.execute(truncate_command)
        log.debug(cur.query)
        cur.execute(insert_command, insert_list)
        log.debug(cur.query)
        conn.commit()
    except Exception as e:
        log.error(f"Failed to set bins in db: {cur.query}")
        raise e
    finally:
        cur.close()


def set_active_nodes(conn, new_active_nodes):
    """
    set_active_nodes
    - 1) retrieves active nodes from the database
    - 2) update any changed entries & insert missing nodes/applications entries, forming delete & insert lists
    - 3) delete nodes that are no longer active
    - 4) insert new active nodes
    :param conn: Database connection object
    :param new_active_nodes: dict of wallet addr -> Identity
    :return:
    """
    cur = conn.cursor()

    # Fetch current set of active nodes
    get_active_nodes = "SELECT wallet_address, id FROM active_nodes;"
    try:
        cur.execute(get_active_nodes)
        log.debug(cur.query)
    except Exception as e:
        log.error(f"Failed to retrieve current set of active nodes: '{cur.query}'")
        cur.close()
        raise e
    active_nodes = cur.fetchall()

    # Loop through current active nodes, form delete list, update where needed
    update_active_node = "UPDATE active_nodes SET id = %s WHERE wallet_address = %s;"
    delete_list = []
    for row in active_nodes:
        wallet = row[0]
        node_id = bytes(row[1])
        if wallet in new_active_nodes.keys():
            identity = new_active_nodes[wallet]
            new_wallet_node_id = identity.cmix_id + b'\x02'
            if node_id == new_wallet_node_id:
                log.debug(f"No change for row {row}")
            else:
                # Update the node id for this wallet
                try:
                    cur.execute(update_active_node, (new_wallet_node_id, wallet))
                    log.debug(cur.query)
                except Exception as e:
                    log.error(f"Failed to update active node: {cur.query}")
                    cur.close()
                    raise e

            # Get node info & update application if needed
            reg_code = id_to_reg_code(identity.cmix_id)
            node_info = get_node_info(conn, reg_code)
            if not node_info:
                log.warning(f"No node or application data found for {reg_code}")
            else:
                update_application_if_needed(conn, identity, node_info)

            del (new_active_nodes[wallet])  # Delete from new active nodes if acted on here
        else:
            delete_list = delete_list + [wallet]

    # Delete nodes that are no longer active
    delete_active_nodes = "DELETE FROM active_nodes WHERE wallet_address in %s;"
    if len(delete_list) > 0:
        try:
            cur.execute(delete_active_nodes, (tuple(delete_list),))  # Execute delete
            log.debug(cur.query)
        except Exception as e:
            log.error(f"Failed to delete from active nodes: '{cur.query}'")
            cur.close()
            raise e

    # Loop through new active nodes to create node/application entries where needed
    insert_application = "INSERT INTO applications (id, name, url, blurb, email, twitter) " \
                         "VALUES (%s, %s, %s, %s, %s, %s);"
    insert_node = "INSERT INTO nodes (code, application_id, status, sequence) VALUES (%s, %s, 2, 'AQ');"
    max_app_id = -1
    nodes = []
    for wallet, identity in new_active_nodes.items():
        reg_code = id_to_reg_code(identity.cmix_id)
        nodes = nodes + [wallet, identity.cmix_id + b'\x02']
        node_info = get_node_info(conn, reg_code)
        if not node_info:  # No entry in nodes for this - need to make one
            # Check max app ID for new applications
            if max_app_id < 0:
                max_app_id = get_max_app_id(conn)
            max_app_id += 1
            # Insert new nodes & applications entries
            try:
                cur.execute(insert_application, (max_app_id, identity.name, identity.url,
                                                 identity.blurb, identity.email, identity.twitter))
                log.debug(cur.query)
                cur.execute(insert_node, (reg_code, max_app_id))
                log.debug(cur.query)
            except Exception as e:
                log.error(f"Failed to insert nodes & applications entries: {cur.query}")
                cur.close()
                raise e
        else:
            update_application_if_needed(conn, identity, node_info)

    # Insert any new active nodes
    if len(nodes) > 0:
        insert_active_nodes = "INSERT INTO active_nodes (wallet_address, id) VALUES"
        insert_active_nodes = insert_active_nodes + (' (%s, %s),' * len(new_active_nodes))
        insert_active_nodes = insert_active_nodes[:-1] + ";"
        try:
            cur.execute(insert_active_nodes, nodes)
            log.debug(cur.query)
        except Exception as e:
            log.error(f"Failed to insert new active nodes: {cur.query}")
            cur.close()
            raise e

    # Commit & exit
    try:
        conn.commit()
    except Exception as e:
        log.error(f"Failed to commit changes from set_active_nodes")
        raise e
    finally:
        cur.close()


def get_max_app_id(conn):
    """
    Get highest app ID from db, or zero if db is empty
    :param conn: Database connection object
    :return: int
    """
    cur = conn.cursor()
    get_max_id = "SELECT MAX(id) FROM applications;"
    try:
        cur.execute(get_max_id)
        log.debug(cur.query)
    except Exception as e:
        log.error(f"Failed to get max app ID: {cur.query}")
        cur.close()
        raise e

    row = cur.fetchone()
    cur.close()
    return int(row[0]) if row and row[0] else 0


def get_authorizer_nodes(conn):
    """
    get list of nodes currently in the authorizer nodes table
    :param conn: authorizer database connection
    :return: list of rows containing id, ip_address, last_updated
    """
    cur = conn.cursor()

    # Get Node information from nodes table
    get_command = "SELECT id, ip_address, last_updated FROM nodes;"
    try:
        cur.execute(get_command)
        log.debug(cur.query)
    except Exception as e:
        log.error(f"Failed to select nodes from db: {get_command}")
        cur.close()
        raise e

    return cur.fetchall()


def set_authorizer_nodes(conn, to_add, to_delete):
    """
    Set nodes in the authorizer db
    :param conn: database connection object
    :param to_add: list of node IDs to add
    :param to_delete: list of node IDs to delete
    :return list[ip_address]: list of IPs to revoke auth
    """
    cur = conn.cursor()

    # Convert Node information into authorizer insert command
    node_list = get_authorizer_nodes(conn)
    to_revoke = []

    delete_command = "DELETE FROM nodes WHERE id = %s;"
    for row in node_list:
        if bytes(row[0]) in to_delete:
            try:
                cur.execute(delete_command, (row[0],))
                log.debug(cur.query)
            except Exception as e:
                log.error(f"Failed to remove node from authorizer DB: {cur.query}")
                raise e
            if row[1]:
                to_revoke.append(row[1])

    if len(to_add) > 0:
        insert_list = [(i, None, None) for i in to_add]
        # Insert Node information into authorizer db
        insert_command = "INSERT INTO nodes (id, ip_address, last_updated) VALUES" + \
                (' (%s, %s, %s),' * len(insert_list))
        insert_command = insert_command[:-1] + " ON CONFLICT DO NOTHING;"
        try:
            cur.execute(insert_command, [e for l in insert_list for e in l])
            log.debug(cur.query)
            conn.commit()
        except Exception as e:
            log.error(f"Failed to insert into authorizer db: {cur.query}")
            raise e
        finally:
            cur.close()

    return to_revoke


def check_table(conn, table_name):
    """
    check_table returns true or false if a table exists in the database
    :param conn: database connection object
    :param table_name: table name string
    :return: boolean
    """
    cur = conn.cursor()
    query = 'select exists(select * from information_schema.tables where table_name=%s);'
    try:
        cur.execute(query, (table_name,))
        log.debug(cur.query)
        return cur.fetchone()[0]
    except Exception as e:
        log.error(f"Failed to check table: {cur.query}")
        raise e
    finally:
        cur.close()


if __name__ == "__main__":
    main()
