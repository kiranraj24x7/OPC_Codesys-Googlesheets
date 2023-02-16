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
nodeid_b_bool_test = 'ns=4;s=|var|CODESYS Control Win V3 x64.Application.PLC_PRG.b_bool_test'
nodeid_i_increment = 'ns=4;s=|var|CODESYS Control Win V3 x64.Application.PLC_PRG.i_increment'
nodeid_i_new_value1 = 'ns=4;s=|var|CODESYS Control Win V3 x64.Application.PLC_PRG.i_new_value1'
nodeid_i_new_value2 = 'ns=4;s=|var|CODESYS Control Win V3 x64.Application.PLC_PRG.i_new_value2'
nodeid_i_sum = 'ns=4;s=|var|CODESYS Control Win V3 x64.Application.PLC_PRG.i_sum'
nodeid_i_value1 = 'ns=4;s=|var|CODESYS Control Win V3 x64.Application.PLC_PRG.i_value1'
nodeid_i_value2 = 'ns=4;s=|var|CODESYS Control Win V3 x64.Application.PLC_PRG.i_value2'
nodeid_s_string_test = 'ns=4;s=|var|CODESYS Control Win V3 x64.Application.PLC_PRG.vc_string_test'
nodeid_i_sum_max = 'ns=4;s=|var|CODESYS Control Win V3 x64.Application.PLC_PRG.i_sum_max'
nodeid_i_sum_trag = 'ns=4;s=|var|CODESYS Control Win V3 x64.Application.PLC_PRG.i_sum_trag'

# powerbi streaming data set URL
pbi_strmdata_url = 'https://api.powerbi.com/beta/b55539a2-16d1-4d55-a3e1-5d9b450363ac/datasets' \
                   '/9f29e8b8-7fa9-489c-ae12-0fee729088dd/rows?key=6ywFrISGWOexiXdfdK%2F4xOddYw7%2F' \
                   'jBmLOid40GI1sZTG4cfjeX%2Ftvr38d76vuR78Ms77aGwHmYPWrhg13rjVRg%3D%3D'

# connecting to client
client = Client("opc.tcp://localhost:4840")
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

# try:
#     client.connect()
#     write_value_bool(nodeid_b_bool_test, False)
#
# finally:
#     client.disconnect()
