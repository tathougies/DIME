{-# LANGUAGE RecordWildCards #-}
module Main where

import Control.Monad

import Database.DIME.Server
import Database.DIME.Server.Config
import Database.DIME.Util

import System.Exit
import System.Environment
import System.Console.GetOpt
import System.IO

data Options = Options {
      optDebug :: Bool
    }

startOptions :: IO Options
startOptions = return Options { optDebug = False }

options :: [OptDescr (Options -> IO Options)]
options = [Option "d" ["debug"]
           (NoArg (\opt -> return opt {optDebug = True}))
           "Enable debug messages",
           Option "h" ["help"]
           (NoArg $ \_ -> do
              prg <- getProgName
              hPutStrLn stderr (usageInfo prg options)
              exitWith ExitSuccess)
           "Show this help"]

main = do
  args <- getArgs

  let (actions, nonOptions, errors) = getOpt RequireOrder options args

  case nonOptions of
    [] -> return ()
    _ -> fail "Extraneous command-line arguments given"

  Options {..} <- foldl (>>=) startOptions actions
  when optDebug $ setDebugMode

  initServerConfig
  serverMain