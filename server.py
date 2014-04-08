import os
import random
import select
import socket
import sys
import time

class Client:
  def __init__(self, handle, addr):
    self.handle = handle
    self.addr = addr
    self.name = None
    self.error = ''
    self.read_buffer = ''

  def write_data(self, data):
    try:
      self.handle.sendall((data + '\n').encode('utf-8'))
    except Exception as e:
      print('Could not send data for client %s: %s' % (self, e))
      return False
    return True

  def write_error(self):
    if self.error:
        print('Client error: %s' % self.error)
        self.write_data('ERR ' + self.error)
        self.error = ''

  def add_data(self, data):
    self.read_buffer += data

  def has_msg(self):
    return self.read_buffer.find('\n') != -1

  def pop_msg(self):
    msg, sep, rest = self.read_buffer.rpartition('\n')
    if msg:
      self.read_buffer = rest 
      return msg.strip()
    else:
      return None

class AuthManager:
  def __init__(self):
    self.name_to_password = {}
    self.used_addrs = set() 

  def register(self, client, name, password):
    print('Register %s %s' % (name, password))
    if client.addr in self.used_addrs and client.addr != '127.0.0.1':
      client.error = 'Only one registration per ip'
      return False
    if name in self.name_to_password:
      client.error = 'Already registered'
      return False
    self.name_to_password[name] = password
    self.used_addrs.add(client.addr)
    return True

  def auth(self, client, name, password):
    print('Client auth %s %s' % (name, password))
    if not name in self.name_to_password:
      client.error = 'Invalid credentials'
      return False
    if self.name_to_password[name] == password:
      client.name = name
      return True
    else:
      client.error = 'Invalid credentials'
      return False

class Game:
  timeout = 1

  def __init__(self, a, b, game_name):
    self.a = a
    self.b = b
    self.a_waiting = None
    self.b_waiting = None
    self.game_name = game_name
    self.finished = False
    self.result = None
    msg = 'SRT %s ' % game_name
    self.a.write_data(msg + self.b.name)
    self.b.write_data(msg + self.a.name)
    print('Game made %s %s' % (a, b))

  def get_ts(self):
    return time.monotonic()    

  def timeout_client(self, client):
    self.finished = True
    self.result = self.get_opposite(client)
    self.a_waiting = None
    self.b_waiting = None

  def update(self):
    print('Update')
    ts = self.get_ts()
    timed_out_client = None 
    if self.a_waiting and ts - self.a_waiting > self.timeout:
      timed_out_client = self.a
    if self.b_waiting and ts - self.b_waiting > self.timeout:
      if not timed_out_client or self.b_waiting < self.a_waiting:
        timed_out_client = self.b
    if timed_out_client:
      print('Client timed out in %s' % self.game_name)
      self.timeout_client(timed_out_client)

  def send_results(self):
    print('Sending results for game %s' % self.game_name)
    s_prefix = 'FIN %s ' % self.game_name
    s_win = s_prefix + 'WIN'
    s_lose = s_prefix + 'LSE'
    s_draw = s_prefix + 'DRW'
    if self.result:
      self.result.write_data(s_win)
      self.get_opposite(self.result).write_data(s_lose)
    else:
      self.a.write_data(s_draw)
      self.b.write_data(s_draw)

  def get_opposite(self, p):
    if p == self.a:
      return self.b
    else:
      return self.a

  def remove_client(self, client):
    if not self.finished:
      self.finished = True
      self.result = self.get_opposite(client)
    
  def handle_data(self, client, tok):
    pass

class KalahGame(Game):
  def __init__(self, a, b, game_name):
    Game.__init__(self, a, b, game_name)
    self.a_waiting = self.get_ts()

class GamePoolManager:
  def __init__(self, game_name, game_class):
    self.game_name = game_name
    self.game_class = game_class
    self.games = set()
    self.stats = {}
    self.clients_not_in_game = set()
    self.client_to_game = {}

  def has_client(self, client):
    return client in self.client_to_game or client in self.clients_not_in_game

  def handle_game_finished(self, game):
    game.send_results()
    self.client_to_game.pop(game.a, None)
    self.client_to_game.pop(game.b, None)

  def update(self):
    for game in self.games:
      game.update()
    self.reap_games()

  def reap_games(self):
    for game in self.games.copy():
      if game.finished:
        print('Reaping game from game pool %s' % self.game_name)
        self.handle_game_finished(game)
        self.games.remove(game)

  def do_pairing(self):
    if len(self.clients_not_in_game) >= 2:
      a, b = random.sample(self.clients_not_in_game, 2)
      self.clients_not_in_game.remove(a)
      self.clients_not_in_game.remove(b)
      game = self.game_class(a, b, self.game_name)
      self.client_to_game[a] = game
      self.client_to_game[b] = game
      self.games.add(game)

  def add_client(self, client):
    if self.has_client(client):
      client.error = 'Already lfg'
      return False
    print('Game pool %s added client' % self.game_name)
    self.clients_not_in_game.add(client)
    self.do_pairing()
    return True

  def remove_client(self, client):
    if self.has_client(client):
      print('Game pool %s removed client' % self.game_name)
    if client in self.client_to_game:
      self.client_to_game[client].remove_client(client)
      del self.client_to_game[client]
    if client in self.clients_not_in_game:
      self.clients_not_in_game.remove(client)
    self.reap_games()

  def send_stats(self, client):
    stats = self.stats.getdefault(client.name, (0, 0, 0))

    client.write_data('%d wins, %d draws, %d losses' % stats)
    return True

  def handle_data(self, client, tok):
    if client not in self.client_to_game:
      client.error = 'Client not in game'
      return False
    result = self.client_to_game[client].handle_data(client, tok)
    self.reap_games()
    return result

class ClientManager:
  def __init__(self):
    self.clients = {}
    self.auth_manager = AuthManager()
    self.game_to_pool_mgr = {'KLH':GamePoolManager('KLH', KalahGame)}

  def update(self):
    for pool_mgr in self.game_to_pool_mgr.values():
      pool_mgr.update()

  def add_client(self, handle, addr):
    self.clients[handle] = Client(handle, addr)

  def remove_client(self, handle):
    for pool_mgr in self.game_to_pool_mgr.values():
      pool_mgr.remove_client(self.clients[handle])
    del self.clients[handle]

  def handle_register(self, client, tok):
    if len(tok) != 3:
      client.error = 'Not enough arguments for command'
      return False
    return self.auth_manager.register(client, tok[1], tok[2])

  def handle_auth(self, client, tok):
    if len(tok) != 3:
      client.error = 'Not enough arguments for command'
      return False
    return self.auth_manager.auth(client, tok[1], tok[2])

  def handle_get_stats(self, client, tok):
    if len(tok) != 3:
      client.error = 'Not enough arguments for command'
      return False
    if tok[1] not in self.game_to_pool_mgr:
      client.error = 'Unrecognised game type'
      return False

    return self.game_to_pool_mgr[tok[1]].send_stats(client)

  def handle_lfg(self, client, tok):
    if len(tok) != 2:
      client.error = 'Not enough arguments for command'
      return False
    if tok[1] not in self.game_to_pool_mgr:
      client.error = 'Unrecognised game type'
      return False

    return self.game_to_pool_mgr[tok[1]].add_client(client)

  def handle_data(self, client, tok):
    if len(tok) < 2:
      client.error = 'Not enough arguments for command'
      return False
    if tok[1] not in self.game_to_pool_mgr:
      client.error = 'Unrecognised game type'
      return False

    return self.game_to_pool_mgr[tok[1]].handle_data(client, tok)

  def handle_msg(self, client, msg):
    if not msg:
      client.error = 'Empty command'
    tok = msg.split(' ')
    if len(tok) == 0:
      client.error = 'Empty command'
      return False
    if tok[0] == "REG":
      return self.handle_register(client, tok)
    if tok[0] == "ATH":
      return self.handle_auth(client, tok)
    if tok[0] == "IFO" and client.name:
      return self.handle_get_stats(client, tok)
    if tok[0] == "LFG" and client.name:
      return self.handle_lfg(client, tok)
    if tok[0] == "DAT" and client.name:
      return self.handle_data(client, tok)
    if client.name:
      client.error = 'Unrecognised command'
    else:
      client.error = 'Client not authed'
    return client.error == ''

  def client_data(self, handle, data):
    client = self.clients[handle]
    client.add_data(data)
    while client.has_msg():
      if not self.handle_msg(client, client.pop_msg()):
        client.write_error()  
        return False
    return True


def main():
  server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  server_socket.bind(('localhost', 31337))
  server_socket.listen(5)

  client_manager = ClientManager()

  sockets = [server_socket]
  while True:
    input_sockets, _, _ = select.select(sockets, [], [], 0.2)
    for input_socket in input_sockets:
      if input_socket == server_socket:
        sock, addr = server_socket.accept()
        addr = addr[0]
        print('Connection from "%s"' % addr)
        sockets.append(sock)
        client_manager.add_client(sock, addr)
      else:
        data = input_socket.recv(4096).decode('utf-8')
        client_okay = client_manager.client_data(input_socket, data)
        if not data or not client_okay:
          print('Client disconnected')
          input_socket.close()
          client_manager.remove_client(input_socket)
          sockets.remove(input_socket)
    client_manager.update()
  server_socket.close()

if __name__ == '__main__':
  main()

