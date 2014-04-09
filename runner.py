import fcntl
import optparse
import os
import select
import shlex
import socket
import subprocess
import sys

def pop_msg(buffer):
  msg, sep, rest = buffer.partition('\n')
  return msg.strip('\n\r'), rest

def send_cmd(server, cmd):
  try:
    print("SEND %s" % (cmd.strip().__repr__()))
    server.sendall((cmd.strip() + '\n').encode('ascii'))
  except:
    return False
  return True

def run_program(server, program, user, game):
  send_cmd(server, 'ATH %s %s' % user)
  send_cmd(server, 'LFG %s' % game)

  process = None
  sockets = [server]
  buffers = {server:''}
  running = True
  while running:
    input_sockets, _, _ = select.select(sockets, [], [])
    for input_socket in input_sockets:
      if not running:
        break

      if input_socket == server:
        data = input_socket.recv(4096)
        if data:
          buffers[input_socket] += data.decode('ascii')
        else:
          print('Server closed connection')
          running = False
      else:
        while True:
          l = input_socket.read(4096)
          if l:
            buffers[input_socket] += l
          else:
            break

    for sock in sockets:
      while running:
        msg, buffers[sock] = pop_msg(buffers[sock])
        if msg:
          if sock == server:
            print('RECV: %s' % msg.__repr__())
            tok = msg.split(' ')
            if tok[0] == 'SRT':
              process = subprocess.Popen(
                  shlex.split(program),
                  stdin=subprocess.PIPE,
                  stdout=subprocess.PIPE,
                  universal_newlines=True,
                  bufsize=0)
              fcntl.fcntl(process.stdout.fileno(), fcntl.F_SETFL, os.O_NONBLOCK)
              sockets.append(process.stdout)
              buffers[process.stdout] = ''
            elif tok[0] == 'FIN':
              running = False
            elif tok[0] == 'DAT':
              process.stdin.write(' '.join(tok[2:]) + '\n')
          else:
            if process and sock == process.stdout:
              msg = 'DAT %s %s' % (game, msg)
            if not send_cmd(server, msg):
              print('Lost connection to server')
              running = False
        else:
          break

  if process:
    process.terminate()
    process.kill()



def register(server, register):
  send_cmd(server, 'REG %s %s' % register)

def main():
  parser = optparse.OptionParser()
  parser.add_option('-s', '--server', dest='server',
                    help='Server location', default='127.0.0.1')
  parser.add_option('-p', '--program', dest='program',
                    help='Program')
  parser.add_option('-u', '--user', nargs=2, dest='user',
                    help='Auth with username and password')
  parser.add_option('-r', '--register', nargs=2, dest='register',
                    help='Register with username and password')
  parser.add_option('-g', '--game',  dest='game', default='KLH',
                    help='Which game to play')
  (options, args) = parser.parse_args()

  server = socket.create_connection((options.server, 31337))
  if options.program:
    run_program(server, options.program, options.user, options.game)
  elif options.register:
    register(server, options.register)
  server.close()

if __name__ == '__main__':
  main()
