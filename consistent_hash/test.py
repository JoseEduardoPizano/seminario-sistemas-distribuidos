#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2022 rzavalet <rzavalet@noemail.com>
#
# Distributed under terms of the MIT license.

from CHash import *
from ModHash import *
from Store import *
"""
Creates a Store that is managed using a consistent hash.

We store a bunch of words in the datastore, then remove one node from the store
and then add it back.
"""


def read_words(fname):
  """
  Just read a bunch of words from a file.
  """
  result = []
  with open(fname, 'r') as f:
    for word in f:
      word = word.split('/')[0]
      word = word.strip()
      result.append(word)

  return result


def run(words):
  """
  We create an object representing the hash scheme that we are willing to use.
  The hash object is then passed to the Store constructor. When adding elements
  to the store, the selected hash scheme is used to determine where to place
  the records.
  """
  my_hash_c = CHash()
  my_store_c = Store(my_hash_c)
  
  my_hash_m = ModHash()
  my_store_m = Store(my_hash_m)
  
  
  """
  Add three nodes to the Store
  """
  my_store_c.add_node("Node 1")
  my_store_c.add_node("Node 2")
  my_store_c.add_node("Node 3")
  
  my_store_c.dump()
  
  my_store_m.add_node("Node 1")
  my_store_m.add_node("Node 2")
  my_store_m.add_node("Node 3")
  
  my_store_m.dump()
  

  """
  Save all words in the Store
  """
  for word in words:
      my_store_c.add_resource(word)
      my_store_m.add_resource(word)
  
  my_store_c.dump()
  my_store_m.dump()


  """
  Remove one node from the Store. Stored objects need to be migrated to the
  remaining nodes.
  """
  my_store_c.remove_node("Node 1")
  my_store_c.dump()
  
  my_store_m.remove_node("Node 1")
  my_store_m.dump()


  """
  Add the node back to the Store. Objects need to be migrated to conform to the
  Hash scheme.
  """
  my_store_c.add_node("Node 1")
  my_store_c.dump()
  
  my_store_m.add_node("Node 1")
  my_store_m.dump()



if __name__ == '__main__':

  words = read_words('words_alpha.txt')
  words = words[:100]

  run(words)
