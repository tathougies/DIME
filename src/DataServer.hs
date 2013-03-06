module Main where

import Control.Monad

import Database.DIME.DataServer
import Database.DIME.DataServer.Config
import Database.DIME.Util

import Network.BSD

import System.Exit
import System.Environment
import System.Console.GetOpt
import System.IO

data Options = Options {
      optDebug :: Bool,
      optLocalAddress :: String
    }

startOptions :: IO Options
startOptions = do
  hostName <- getHostName
  return Options {optDebug = False, optLocalAddress = hostName}

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
            "Show this help",
            Option "l" ["local-address"]
            (OptArg
             (\arg opt -> case arg of
                            Nothing -> do
                              hostName <- getHostName
                              return opt { optLocalAddress = hostName }
                            Just localAddress -> do
                                putStrLn $ "Got local address " ++ show localAddress
                                return opt { optLocalAddress = localAddress })
             "<local-address>")
            "Set the local address"
          ]

main = do
  args <- getArgs

  let (actions, nonOptions, errors) = getOpt RequireOrder options args

  -- Run the actions on the options
  opts <- foldl (>>=) startOptions actions

  coordinatorName <- case nonOptions of
                       [] -> fail "A coordinator name must be passed into the program"
                       x -> return $ head x

  let Options { optDebug = debug, optLocalAddress = localAddress } = opts
  when debug $ setDebugMode

  -- Read config files
  initDataServerConfig

  dataServerMain coordinatorName localAddress