module Main where

import Database.DIME.Server
import Database.DIME.Server.Config

main = do
  initServerConfig
  serverMain