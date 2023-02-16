import requests
from opcua import Client, ua


def read_input_value(node_id):
    client_node = client.get_node(node_id)  # get node
    client_node_value = client_node.get_value()  # read node value
    return str(client_node_value)


def write_value_int(node_id, value):
    client_node = client.get_node(node_id)  # get node
    client_node_value = value
    client_node_dv = ua.DataValue(ua.Variant(client_node_value, ua.VariantType.Int16))
    client_node.set_value(client_node_dv)


def write_value_bool(node_id, value):
    client_node = client.get_node(node_id)  # get node
    client_node_value = value
    client_node_dv = ua.DataValue(ua.Variant(client_node_value, ua.VariantType.Boolean))
    client_node.set_value(client_node_dv)


def write_value_string(node_id, value):
    client_node = client.get_node(node_id)  # get node
    client_node_value = value
    client_node_dv = ua.DataValue(ua.Variant(client_node_value, ua.VariantType.String))
    client_node.set_value(client_node_dv)



# node id for all the variables
nodeid_b_bool_test = '''This can be ontained using ua client gui or Ua expert'''

# powerbi streaming data set URL
pbi_strmdata_url = 'URL from power BI'

# connecting to client
client = Client("from ua expert")
client.connect()

while True:

    while True:  # streaming data set value pushing
        stream_json = [
            {
                "activate": 'read_input_value(nodeid_b_bool_test)',
                "comment": 'read_input_value(nodeid_s_string_test)',
                "sum": int(read_input_value(nodeid_i_sum)),
                "target": int(read_input_value(nodeid_i_sum_trag)),
                "max": int(read_input_value(nodeid_i_sum_max)),
                "increment": int(read_input_value(nodeid_i_increment))
            }
        ]
        requests.post(pbi_strmdata_url, json=stream_json)


