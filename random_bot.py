"""
Mancala game v1.0
Lyndon While, 3 April 2014

The program prompts the user for the game size X, then it plays a
game of Mancala between two random players and creates an SVG file

  MancalaGameX.svg

that shows the history of the board during the game. Each board is
annotated with the next move made, and the final board is annotated
with the average branching factor during the game.

Software to run the program is available from python.org. Get Python 3.

The size of the displayed boards and the fonts used can be controlled
by changing the variable size below.

The colours used in the display can be controlled by changing the
variable colours.

Please report any bugs on help3001. Unless they're embarrassing ones. :-)
"""

import random
import copy
import sys

#a board position is a 2x7 list with the stores at the ends
#a player is 0 or 1, and is used to index the board
#a move is a list of indices into a board

#------------------------------------------------------------- This is the display code

size = 5 #controls the board-size and fonts - don't change anything else

side    = size * 5
housefont = size * 2
storefont = size * 3

colours = [(0, 0, 0), (255,150,150), (215,215,0)]
#      black    pink       green-yellow

def mkhouse(k, p, x, r):
  h = side * (3.4 + k + r // 10 * 9)
  v = side * (1.5 - p + r % 10 * 3)
  return (colours[p + 1], [(h, v), (h + side, v), (h + side, v + side), (h, v + side), (h, v)],
      #text placement and font
      (h + side / 2 - len(str(x)) * side / 8, v + 2 * side / 3, str(x), housefont))

def mkstore(p, x, r):
  h = side * (9.4 - 7 * p + r // 10 * 9)
  v = side * (0.5 + r % 10 * 3)
  return (colours[p + 1], [(h, v), (h + side, v), (h + side, v + 2 * side), (h, v + 2 * side), (h, v)],
      #text placement and font
      (h + side / 2 - len(str(x)) * side / 5, v + 6 * side / 5, str(x), storefont))

def writeColor(c):
  (r, g, b) = c
  return "".join(["rgb(", str(r), ",", str(g), ",", str(b), ")"])

def writeText(t):
  (h, v, z, s) = t
  return "".join(["<text x=\"", str(h), "\" y=\"", str(v), "\" font-family=\"Verdana\" font-size=\"",
           str(s), "\" fill=\"black\">", z, "</text>\n"])

def writePolygons(f, ps):
  for (c, p, t) in ps:
    f.write("<polygon points=\"")
    for (x, y) in p:
      f.write("".join([str(x), ",", str(y), " "]))
    f.write("\" style=\"fill:")
    f.write(writeColor(c))
    f.write(";stroke:")
    f.write(writeColor(colours[0]))
    f.write(";stroke-width:3\"/>\n")
    f.write(writeText(t))

def mancalaDisplay(b, m, r, f):
  if r % 2 == 1: t = "green"
  else:      t = "pink"
  if r < 10:
    f.write(writeText((size, side * (1.0 + r % 10 * 3), t + "'s", housefont)))
    f.write(writeText((size, side * (1.5 + r % 10 * 3), "move", housefont)))
  #display the move
  f.write(writeText((size + side * (2 + (r - 1) // 10 * 9), side * (3 + (r - 1) % 10 * 3), "".join([str(k + 1) for k in m]), housefont)))
  writePolygons(f, [mkhouse(k, p, b[p][5 * p + k * (1 - 2 * p)], r) for p in range(2) for k in range(6)] +
           [mkstore(   p, b[p][6],             r) for p in range(2)])

#------------------------------------------------------------- This is the game mechanics code

def moves(b, p):
#returns a list of legal moves for player p on board b
  zs = []
  #for each non-empty house on p's side
  for m in [h for h in range(6) if b[p][h] > 0]:
    #if the final seed will be sown in p's store
    if (b[p][m] + m) % 13 == 6:
      #copy b, make move m, and check for recursive possibilities
      c = copy.deepcopy(b)
      move(c, p, [m])
      ms = moves(c, p)
      if ms == []:
        zs += [[m]]
      else:
        zs += [[m] + n for n in ms]
    else:
      zs += [[m]]
  return zs

def move(b, p, ms):
#make the move ms for player p on board b
  for m in ms:
    x = b[p][m]
    b[p][m] = 0
    (capturePossible, z) = sow(b, p, m + 1, 6, x)
  #if the last seed was sown in an empty house on p's side, with seeds opposite
  if capturePossible and b[p][z] == 1 and b[1 - p][5 - z] > 0:
    b[p][6] += b[p][z] + b[1 - p][5 - z]
    b[p][z] = 0
    b[1 - p][5 - z] = 0

def sow(b, p, m, y, x):
#sow x seeds for player p on board b, starting from house m, with limit y
#the limit is used to exclude the opponent's store
#it returns (possibleCapture, lastHouseSown)
  while x > 0:
    for z in range(m, min(y + 1, m + x)):
      b[p][z] += 1
    x -= y + 1 - m
    p = 1 - p
    m = 0
    y = 11 - y
  return (y == 5, z)

def mancala(n):
  b = [[n] * 6 + [0] for p in [0, 1]]
  bmps = 0
  while True:
    s = [i.strip() for i in sys.stdin.readline().split(' ')]
    if len(s) == 0:
      continue
    if s[0] == 'BMP':
      bmps += 1
      if bmps == 1:
        ms = moves(b, 0)
        m = random.choice(ms)
        bmps -= len(m)
        for i in m:
          print('MOV %d' % i)
          sys.stdout.flush()
        move(b, 0, m)
    elif s[0] == 'MOV':
      mov = int(s[1]) - 7
      waiting = (b[1][mov] + mov) % 13 == 6
      move(b, 1, [mov])
    else:
      break
    #mancalaDisplay(b, m, r, f)

    sys.stderr.write(str(b) + '\n')

def main():
  mancala(int(3))

main()
