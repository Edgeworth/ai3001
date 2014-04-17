import hashlib
import os
import random
import select
import socket
import sys
import time
import traceback

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

class Client:
  def __init__(self, handle, addr):
    self.handle = handle
    self.addr = addr
    self.name = None
    self.error = ''
    self.read_buffer = ''

  def write_data(self, data):
    try:
      self.handle.sendall((data + '\n').encode('ascii'))
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
    msg, sep, rest = self.read_buffer.partition('\n')
    if msg:
      self.read_buffer = rest
      msg = msg.strip()
      return msg.strip()
    else:
      return None

class AuthManager:
  def __init__(self, users_collection):
    self.name_to_password = {}
    self.users_collection = users_collection

  def register(self, client, name, password):
    print('Register %s' % (name))
    if (self.users_collection.find({'ip_address':client.addr}).count() != 0 and
        client.addr != '127.0.0.1'):
      client.error = 'Only one registration per ip'
      return False
    if len(name) > 20:
      client.error = 'Names must be no more than 20 characters'
      return False
    try:
      password_digest = hashlib.sha512()
      password_digest.update(password.encode('ascii'))
      self.users_collection.insert({
        'username': name,
        'password_digest': password_digest.hexdigest(),
        'ip_address': client.addr,
        'scores': []
      })
      return True
    except DuplicateKeyError:
      client.error = 'Already registered'
      return False

  def auth(self, client, name, password):
    print('Client auth %s' % (name))
    password_digest = hashlib.sha512()
    password_digest.update(password.encode('ascii'))
    user = self.users_collection.find_one({'username':name})
    if user == None:
      client.error = 'Invalid credentials'
      return False
    if password_digest.hexdigest() == user['password_digest']:
      client.name = name
      return True
    else:
      client.error = 'Invalid credentials'
      return False

class Game:
  timeout = 10

  def __init__(self, a, b, game_name):
    self.a = a
    self.b = b
    self.a.waiting = None
    self.b.waiting = None
    self.game_name = game_name
    self.finished = False
    self.result = None
    msg = 'SRT %s ' % game_name
    self.a.write_data(msg + self.b.name)
    self.b.write_data(msg + self.a.name)
    print('Game made %s %s' % (self.a.name, self.b.name))

  def get_ts(self):
    return time.monotonic()

  def client_won(self, client):
    self.finished = True
    self.result = client
    self.a.waiting = None
    self.b.waiting = None

  def update(self):
    ts = self.get_ts()
    timed_out_client = None
    if self.a.waiting and ts - self.a.waiting > self.timeout:
      timed_out_client = self.a
    if self.b.waiting and ts - self.b.waiting > self.timeout:
      if not timed_out_client or self.b.waiting < self.a.waiting:
        timed_out_client = self.b
    if timed_out_client:
      print('Client timed out in %s' % self.game_name)
      self.client_won(self.get_opposite(timed_out_client))

  def send_results(self):
    win_name = 'noone'
    if self.result:
      win_name = self.result.name
    print('Sending results for game %s: %s won' % (self.game_name, win_name))
    g_prefix = 'DAT %s ' % self.game_name
    s_prefix = 'FIN %s ' % self.game_name
    win_str = 'WIN'
    lose_str = 'LSE'
    draw_str =  'DRW'
    if self.result:
      win = self.result
      lose = self.get_opposite(self.result)
      win.write_data(g_prefix + win_str)
      lose.write_data(g_prefix + lose_str)
      win.write_data(s_prefix + win_str)
      lose.write_data(s_prefix + lose_str)
    else:
      self.a.write_data(g_prefix + draw_str)
      self.b.write_data(g_prefix + draw_str)
      self.a.write_data(s_prefix + draw_str)
      self.b.write_data(s_prefix + draw_str)

  def get_opposite(self, p):
    if p == self.a:
      return self.b
    else:
      return self.a

  def remove_client(self, client):
    if not self.finished:
      self.finished = True
      self.result = self.get_opposite(client)

  def client_data(self, client, tok):
    if not self.handle_data(client, tok):
      self.client_won(self.get_opposite(client))
      return False
    if self.has_won():
      self.client_won(self.winner())
    return True

  def handle_data(self, client, tok):
    return True

  def has_won(self):
    return False

  def winner(self):
    return None

class KalahGame(Game):
  a_store = 6
  b_store = 13

  def __init__(self, a, b, game_name):
    Game.__init__(self, a, b, game_name)
    self.board = [3] * 14
    self.board[self.a_store] = 0
    self.board[self.b_store] = 0
    self.a.low_idx = 0
    self.a.high_idx = 7
    self.a.store = self.a_store
    self.b.low_idx = 7
    self.b.high_idx = 14
    self.b.store = self.b_store
    self.wait_for_client(self.a)

  def handle_data(self, client, tok):
    if len(tok) != 4:
      client.error = 'Malformed command'
      return False
    cmd, pos = tok[2], int(tok[3])
    pos = self.normalise_pos_for_client(client, pos)
    if cmd != 'MOV':
      client.error = 'Malformed command'
      return False
    if pos < client.low_idx or pos >= client.high_idx:
      client.error = 'OOB index'
      return False
    if self.board[pos] == 0:
      client.error = 'Must move non-zero number of seeds'
      return False
    if not self.client_owns_house(client, pos):
      client.error = 'Must move own seeds'
      return False
    if not client.waiting:
      client.error = 'Not your turn'
      return False
    client.waiting = None
    opposite_client = self.get_opposite(client)
    if self.move_seeds(client, pos):
      self.update_client(opposite_client, pos)
      if not self.has_won():
        self.wait_for_client(client)
    else:
      self.update_client(opposite_client, pos)
      if not self.has_won():
        self.wait_for_client(opposite_client)
    self.a.write_data(self.print_board(self.a))
    self.b.write_data(self.print_board(self.b))
    return True

  def normalise_pos_for_client(self, client, pos):
    if not self.client_owns_house(client, pos):
      return (pos + 7) % 14
    return pos

  def print_board(self, client):
    opp = self.get_opposite(client)
    top_str = ' '.join(
        str(i) for i in reversed(self.board[opp.low_idx:opp.high_idx - 1]))
    bot_str = ' '.join(
        str(i) for i in self.board[client.low_idx:client.high_idx - 1])
    stores = [self.board[self.b_store], self.board[self.a_store]]
    if client != self.a:
      stores.reverse()
    return ' %s\n%d%s%d\n %s\n' % (
        top_str, stores[0], ' ' * len(top_str), stores[1], bot_str)

  def move_seeds(self, client, pos):
    num_seeds = self.board[pos]
    self.board[pos] = 0

    npos = pos
    for i in range(num_seeds):
      npos = (npos + 1) % 14
      if npos == self.skip_store(client):
        npos = (npos + 1) % 14
      self.board[npos] += 1
    if not self.is_store(npos):
      opp = self.get_opposite_house(npos)
      if (self.client_owns_house(client, npos) and
          self.board[npos] == 1 and self.board[opp] > 0):
        self.board[client.store] += self.board[opp] + 1
        self.board[npos] = 0
        self.board[opp] = 0
    else:
      return True
    return False

  def get_opposite_house(self, pos):
    if pos == self.a_store:
      return self.b_store
    if pos == self.b_store:
      return self.a_store
    return abs(pos - 12)

  def skip_store(self, client):
    if client.store == self.a_store:
      return self.b_store
    return self.a_store

  def is_store(self, pos):
    return pos == self.a_store or pos == self.b_store

  def client_owns_house(self, client, pos):
    return pos >= client.low_idx and pos < client.high_idx

  def get_points(self):
    return (sum(self.board[self.a.low_idx:self.a.high_idx]),
            sum(self.board[self.b.low_idx:self.b.high_idx]))

  def has_won(self):
    a_pts, b_pts = self.get_points()
    return a_pts == self.board[self.a_store] or b_pts == self.board[self.b_store]

  def winner(self):
    a_pts, b_pts = self.get_points()
    if a_pts > b_pts:
      return self.a
    elif a_pts < b_pts:
      return self.b
    return None

  def wait_for_client(self, client):
    client.write_data('DAT %s BMP' % self.game_name)
    client.waiting = self.get_ts()

  def update_client(self, client, pos):
    npos = self.normalise_pos_for_client(self.b, pos)
    client.write_data('DAT %s MOV %d' % (self.game_name, npos))

class GamePoolManager:
  def __init__(self, game_name, game_class, users_collection):
    self.game_name = game_name
    self.game_class = game_class
    self.games = set()
    self.stats = {}
    self.clients_not_in_game = set()
    self.client_to_game = {}
    self.users_collection = users_collection

  def has_client(self, client):
    return client in self.client_to_game or client in self.clients_not_in_game

  def handle_game_finished(self, game):
    self.users_collection.update(
        {
          'username': {'$in': [game.a.name, game.b.name]},
          'scores.game': {'$ne': self.game_name}
        },
        {
          '$addToSet':
            {'scores':
              {'game': self.game_name, 'wins': 0, 'draws': 0, 'losses': 0}
            }
        },
        multi=True
    )

    if game.result:
      winner = game.result
      loser = game.get_opposite(game.result)

      self.users_collection.update(
        {'username': winner.name, 'scores.game': self.game_name},
        {'$inc': {'scores.$.wins': 1}}
      )
      self.users_collection.update(
        {'username': loser.name, 'scores.game': self.game_name},
        {'$inc': {'scores.$.losses': 1}}
      )
    else:
      self.users_collection.update(
        {
          'username':
            {'$in': [game.a.name, game.b.name]},
          'scores.game':
            self.game_name
        },
        {'$inc': {'scores.$.draws': 1}},
        multi=True
      )
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

  def send_scoreboard(self, client):
    scores_cursor = self.users_collection.find(
        {'scores.game':self.game_name},
        {'username':1, 'scores.$':1}
    )
    if scores_cursor.count() == 0:
      client.write_data('BRD FIN')
      return True
    scores = []
    for s in scores_cursor:
      score = s['scores'][0]
      scores.append((score['wins'], score['draws'], score['losses'], s['username']))
    align = max(max(len(k[3]) for k in scores), 4)
    name_str = '%%%ds' % align
    header = '%s   %3s   %3s   %3s' % (name_str % 'NAME', 'WIN', 'DRW', 'LSE')
    print_str = '%s %%5d %%5d %%5d' % name_str
    sorted_stats = sorted(scores, reverse=True)
    stats = '\n'.join(print_str % (i[3], i[0], i[1], i[2]) for i in sorted_stats)
    client.write_data(header)
    client.write_data(stats)
    client.write_data('BRD FIN')
    return True

  def send_stats(self, client):
    score = self.users_collection.find_one(
        {'username': client.name, 'scores.game': self.game_name},
        {'scores.$': 1}
    )

    if score == None:
      stats = (0,0,0)
    else:
      score = score['scores'][0]
      stats = (score['wins'], score['draws'], score['losses'])

    client.write_data('%d wins, %d draws, %d losses' % stats)
    return True

  def handle_data(self, client, tok):
    if client not in self.client_to_game:
      client.error = 'Client not in game'
      return False
    result = self.client_to_game[client].client_data(client, tok)
    self.reap_games()
    return result

class ClientManager:
  commands = ['REG', 'ATH', 'IFO', 'LFG', 'DAT', 'BRD']

  def __init__(self, users_collection):
    self.clients = {}
    self.auth_manager = AuthManager(users_collection)
    self.game_to_pool_mgr = {'KLH':GamePoolManager('KLH', KalahGame, users_collection)}

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
      client.error = 'Wrong number of arguments for command'
      return False
    return self.auth_manager.register(client, tok[1], tok[2])

  def handle_auth(self, client, tok):
    if len(tok) != 3:
      client.error = 'Wrong number of arguments for command'
      return False
    return self.auth_manager.auth(client, tok[1], tok[2])

  def handle_scoreboard(self, client, tok):
    if len(tok) != 2:
      client.error = 'Wrong number of arguments for command'
      return False
    if tok[1] not in self.game_to_pool_mgr:
      client.error = 'Unrecognised game type'
      return False

    return self.game_to_pool_mgr[tok[1]].send_scoreboard(client)

  def handle_get_stats(self, client, tok):
    if len(tok) != 2:
      client.error = 'Wrong number of arguments for command'
      return False
    if tok[1] not in self.game_to_pool_mgr:
      client.error = 'Unrecognised game type'
      return False

    return self.game_to_pool_mgr[tok[1]].send_stats(client)

  def handle_lfg(self, client, tok):
    if len(tok) != 2:
      client.error = 'Wrong number of arguments for command'
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
    if tok[0] == 'REG':
      return self.handle_register(client, tok)
    if tok[0] == 'ATH':
      return self.handle_auth(client, tok)
    if tok[0] == 'BRD':
      return self.handle_scoreboard(client, tok)
    if tok[0] == 'IFO' and client.name:
      return self.handle_get_stats(client, tok)
    if tok[0] == 'LFG' and client.name:
      return self.handle_lfg(client, tok)
    if tok[0] == 'DAT' and client.name:
      return self.handle_data(client, tok)
    if tok[0] in self.commands:
      if not client.name:
        client.error = 'Client not authed'
      else:
        client.error = 'Unknown error'
    else:
      client.error = 'Unrecognised command'
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
  server_socket.bind(('', 31337))
  server_socket.listen(5)

  database_client = MongoClient('localhost', 27017)
  database = database_client['ai3001']

  users_collection = database['users']
  users_collection.ensure_index('username', unique=True)

  client_manager = ClientManager(users_collection)

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
        success = False
        try:
          data = input_socket.recv(4096)
          if data:
            data = data.decode('ascii')
            client_manager.client_data(input_socket, data)
          else:
            print('Client disconnected')
          success = data
        except Exception as e:
          print(traceback.format_exc())
        if not success:
          input_socket.close()
          client_manager.remove_client(input_socket)
          sockets.remove(input_socket)
    client_manager.update()
  server_socket.close()

if __name__ == '__main__':
  main()

