import fcntl
import optparse
import os
import queue
import shlex
import socket
import subprocess
import sys
import threading

def read_blocking(q, f):
  try:
    while f.readable():
      l = f.readline()
      if l:
        if not isinstance(l, str):
          l = l.decode('ascii')
        print('RECV %s' % l.__repr__())
        q.put((f, l.strip()))
      else:
        break
  except:
    pass
  q.put((f, None))

def pop_msg(buffer):
  msg, sep, rest = buffer.partition('\n')
  return msg.strip('\n\r'), rest

def send_cmd(server, cmd):
  try:
    print("SEND %s" % cmd.strip().__repr__())
    server.write(cmd.strip() + '\n')
    server.flush()
  except Exception as e:
    print(e)
    return False
  return True

def run_program(server, program, user, game):
  send_cmd(server, 'ATH %s %s' % user)
  send_cmd(server, 'LFG %s' % game)

  q = queue.Queue()
  t_server = threading.Thread(target=read_blocking, args=(q, server))
  t_server.daemon = True
  t_server.start()
  t_process = None
  process = None
  running = True
  while running:
    f, msg = q.get(timeout=1000000)

    if msg is None:
      if f == server:
        print('Closed connection to server' % f)
        running = False
      elif process and f == process.stdout:
        print('Closed connection to program')
        continue

    if f == server:
      tok = msg.split(' ')
      if tok[0] == 'SRT':
        process = subprocess.Popen(
            shlex.split(program),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            universal_newlines=True,
            bufsize=0)
        t_process = threading.Thread(target=read_blocking,  args=(q, process.stdout))
        t_process.daemon = True
        t_process.start()
      elif tok[0] == 'FIN':
        print("FINISHING")
        running = False
      elif tok[0] == 'DAT':
        dat = ' '.join(tok[2:]) + '\n'
        process.stdin.write(dat)
        process.stdin.flush()
    else:
      if process and f == process.stdout:
        msg = 'DAT %s %s' % (game, msg)
      if not send_cmd(server, msg):
        print('Lost connection to server')
        running = False
  if process:
    process.communicate()


def register(server, register):
  send_cmd(server, 'REG %s %s' % register)

def get_info(server, game, user):
  send_cmd(server, 'ATH %s %s' % user)
  send_cmd(server, 'IFO %s' % game)
  print(server.readline().strip())

def get_board(server, game):
  send_cmd(server, 'BRD %s' % game)
  while True:
    l = server.readline().replace('\n', '')
    if l and l != 'BRD FIN':
      print(l)
    else:
      break

def main():
  parser = optparse.OptionParser()
  parser.add_option('-s', '--server', dest='server',
                    help='Server location', default='127.0.0.1')
  parser.add_option('-p', '--program', dest='program',
                    help='Program')
  parser.add_option('-u', '--user', nargs=2, dest='user',
                    help='Auth with username and password')
  parser.add_option('-i', '--info', dest='info', action='store_true',
                    help='Get info for a client. Requires --user and --game option')
  parser.add_option('-b', '--board', dest='board', action='store_true',
                    help='Get scoreboard. Requires --game option')
  parser.add_option('-r', '--register', nargs=2, dest='register',
                    help='Register with username and password')
  parser.add_option('-g', '--game',  dest='game', default='KLH',
                    help='Which game to play')
  (options, args) = parser.parse_args()

  server = socket.create_connection((options.server, 31337))
  server_file = server.makefile('rw', encoding='ascii')
  if options.program and options.user and options.game:
    run_program(server_file, options.program, options.user, options.game)
  elif options.register:
    register(server_file, options.register)
  elif options.info and options.game and options.user:
    get_info(server_file, options.game, options.user)
  elif options.board and options.game:
    get_board(server_file, options.game)
  else:
    print('Incorrect command')
  server.close()

if __name__ == '__main__':
  main()
