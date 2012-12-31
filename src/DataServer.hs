module Main where

import Database.DIME.DataServer
import Database.DIME.Util

import System.Exit
import System.Environment
import System.Console.GetOpt
import System.IO

import Control.Monad

data Options = Options {
      optDebug :: Bool
    }

startOptions :: Options
startOptions = Options {optDebug = False}

options :: [OptDescr (Options -> IO Options)]
options = [ Option "d" ["debug"]
            (NoArg
             (\opt -> return opt {optDebug = True}))
            "Enable debug messages",
            Option "h" ["help"]
            (NoArg
             (\_ -> do
                prg <- getProgName
                hPutStrLn stderr (usageInfo prg options)
                exitWith ExitSuccess))
            "Show this help"
          ]

main = do
  args <- getArgs

  let (actions, nonOptions, errors) = getOpt RequireOrder options args

  -- Run the actions on the options
  opts <- foldl (>>=) (return startOptions) actions

  let Options { optDebug = debug } = opts

  when debug $ setDebugMode

  dataServerMain