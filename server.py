import thriftpy2
import argparse
from thriftpy2.rpc import make_server
from chord_simulation.implement.chord_finger_table import ChordNode as ChordNodeFingerTable

chord_thrift = thriftpy2.load('chord_simulation/idl/chord.thrift', module_name='chord_thrift')

parser = argparse.ArgumentParser(description='server node for chord simulation.')
parser.add_argument('-a', '--address', type=str, default='localhost', help='server address')
parser.add_argument('-p', '--port', type=int, help='server port')

if __name__ == '__main__':
    args = parser.parse_args()
    node = ChordNodeFingerTable(args.address, args.port)

    server = make_server(chord_thrift.ChordNode, node, args.address, args.port)
    server.serve()
