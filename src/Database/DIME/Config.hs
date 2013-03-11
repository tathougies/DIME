module Database.DIME.Config
    (setDefaults,

     getGlobalConfig,
     getGlobalValue,

     configValue,

     coordinatorPort,
     queryBrokerPort,
     dataPort,
     webPort,

     initConfig,
     readConfigFile) where

import qualified Control.Exception as E
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.Error

import qualified Data.ConfigFile as CP
import Data.Either.Utils

import System.IO.Unsafe

{-# NOINLINE configVar #-}
configVar :: TVar CP.ConfigParser
configVar = unsafePerformIO $ do
              cp <- runErrorT $ do
                                let cp = CP.emptyCP
                                cp <- CP.add_section cp "DIME"
                                cp <- CP.set cp "DIME" "coordinator-port" "8009"
                                cp <- CP.set cp "DIME" "query-broker-port" "8010"
                                cp <- CP.set cp "DIME" "data-port" "8008"
                                cp <- CP.set cp "DIME" "web-port" "8000"
                                return cp
              newTVarIO $ forceEither cp

coordinatorPort, queryBrokerPort, dataPort, webPort :: Int
coordinatorPort = configValue "DIME" "coordinator-port"
queryBrokerPort = configValue "DIME" "query-broker-port"
dataPort = configValue "DIME" "data-port"
webPort = configValue "DIME" "web-port"

setDefaults :: [(CP.SectionSpec, CP.OptionSpec, String)] -> IO ()
setDefaults options =
  atomically $
         forM_ options $ (\(section, option, value) ->
             modifyTVar configVar $ (\config ->
                 let config' = if CP.has_section config section then
                                   config
                                else
                                   forceEither $ CP.add_section config section
                 in forceEither $ CP.set config' section option value))

getGlobalConfig :: IO CP.ConfigParser
getGlobalConfig = atomically $ readTVar configVar

getGlobalValue :: CP.Get_C a => CP.SectionSpec -> CP.OptionSpec -> IO a
getGlobalValue section option = do
  config <- getGlobalConfig
  let result = CP.get config section option
  case result of
    Left e -> fail $ "getGlobalValue: Could not find " ++ option ++ " in " ++ section ++ ": " ++ show e
    Right value -> return value

configValue :: CP.Get_C a => CP.SectionSpec -> CP.OptionSpec -> a
configValue section option = unsafePerformIO $ getGlobalValue section option

readConfigFile :: FilePath -> IO ()
readConfigFile configFile =
    E.handle (\(e :: E.IOException) -> return ()) $ -- if there was a problem reading the file, ignore it...
     do
       cpData <- CP.readfile CP.emptyCP configFile
       case cpData of
         Right newData -> atomically $ modifyTVar configVar (\config -> CP.merge config newData)
         Left e -> error $ "readConfigFile: " ++ show e

initConfig :: IO ()
initConfig = do
  readConfigFile "dime.cfg"
  readConfigFile "/etc/dime.cfg"