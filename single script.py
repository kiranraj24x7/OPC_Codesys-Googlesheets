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

while True:

    client.connect() # connecting to client
    df = pd.read_csv(gsheet_url)
    increment = int(df.iloc[0]['Increment'])
    activate = df.iloc[0]['Activate']
    targetvalue = int(df.iloc[0]['Target_value'])
    max_value = int(df.iloc[0]['Max_value'])
    comment = df.iloc[0]['Comment']
    write_value_string(nodeid_s_string_test, 'comment')
# checking there is a change in the google sheet columns, if there is a change then update the change to codesys
    if increment != prev_increment or activate != prev_activate or targetvalue != prev_targetvalue or max_value != prev_maxvalue or comment != prev_comment:
        write_value_int(nodeid_i_increment, increment)
        write_value_bool(nodeid_b_bool_test, bool(activate))
        write_value_int(nodeid_i_sum_trag, targetvalue)
        write_value_int(nodeid_i_sum_max, max_value)
        write_value_string(nodeid_s_string_test, comment)
        prev_increment = increment
        prev_activate = activate
        prev_targetvalue = targetvalue
        prev_maxvalue = max_value
        prev_comment = comment
# posting data to the streaming data in power bi 
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
    client.disconnect()


