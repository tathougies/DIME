module Main where

import Control.Monad
import Control.Concurrent

import Database.DIME.Server
import Database.DIME.Server.Config
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
      optMaster :: Bool,
      optLocalAddress :: String
    }

startOptions :: IO Options
startOptions = do
  hostName <- getHostName
  return Options {optDebug = False, optMaster = False, optLocalAddress = hostName}

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
            Option "m" ["master"]
            (NoArg
             (\opt -> return opt {optMaster = True}))
            "Designate this node as master",
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

  let Options { optDebug = debug, optMaster = isMaster, optLocalAddress = localAddress } = opts

  coordinatorName <- case nonOptions of
                       [] -> if isMaster
                                then return localAddress
                                else fail "A coordinator name must be passed into the program"
                       x -> return $ head x
  when debug $ setDebugMode

  when isMaster $ do
    initServerConfig
    forkIO serverMain
    return ()

  -- Read config files
  initDataServerConfig

  dataServerMain coordinatorName localAddress