-- | This is a magical module that parses and presents the DIME configuration to the runtime
-- system. The module requires that the main program call the `readConfigFile' (alternatively, the
-- `initConfig') function and optionally, the `setDefaults' function to initialize the data. From
-- this point on, config values can be fetched as norma haskell values.
--
-- It is important to /not/ use these these configured values until after the config file has been
-- read. Also, once the values are used, subsequent calls to `readConfigFile' will no longer affect
-- the values, because the thunks representing the values will already have been computed.
module Database.DIME.Config
    (-- * Option Access
     getGlobalConfig,
     getGlobalValue,
     configValue,

     -- * Convenience Methods
     coordinatorPort,
     queryBrokerPort,
     dataPort,
     webPort,

     -- * Initialization
     setDefaults,
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
-- | The port number for the coordinator server, specifed under section "DIME" and option
-- "coordinator-port"
coordinatorPort = configValue "DIME" "coordinator-port"

-- | The port number for the query broker on the coordinator server, specified under section "DIME"
-- and option "query-broker-port"
queryBrokerPort = configValue "DIME" "query-broker-port"

-- | The port number for the data server, specified under section "DIME" and option "data-port"
dataPort = configValue "DIME" "data-port"

-- | The port number for the web server, specified under section "DIME" and option "web-port"
webPort = configValue "DIME" "web-port"

-- | Call this before calling `readConfigFile' to set default values. If the config files do not
-- contain new values for these options, these values will be used instead. The first argument is
-- a tuple of the form (SectionName, OptionName, Value)
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

-- | Get the global config parser
getGlobalConfig :: IO CP.ConfigParser
getGlobalConfig = atomically $ readTVar configVar

-- | Get a configuration value by specifying the section and option name in the IO monad
getGlobalValue :: CP.Get_C a => CP.SectionSpec -> CP.OptionSpec -> IO a
getGlobalValue section option = do
  config <- getGlobalConfig
  let result = CP.get config section option
  case result of
    Left e -> fail $ "getGlobalValue: Could not find " ++ option ++ " in " ++ section ++ ": " ++ show e
    Right value -> return value

-- | Get a configuration value by specifying section nad option name. This suffers from the same
-- limitations as using the convenience variables.
configValue :: CP.Get_C a => CP.SectionSpec -> CP.OptionSpec -> a
configValue section option = unsafePerformIO $ getGlobalValue section option

-- | Read a config file and merge its options into the global config. Call this function before
-- using `configValue' or any of the convenience functions.
readConfigFile :: FilePath -> IO ()
readConfigFile configFile =
    E.handle (\(e :: E.IOException) -> return ()) $ -- if there was a problem reading the file, ignore it...
     do
       cpData <- CP.readfile CP.emptyCP configFile
       case cpData of
         Right newData -> atomically $ modifyTVar configVar (\config -> CP.merge config newData)
         Left e -> error $ "readConfigFile: " ++ show e


-- | Read the default DIME configuration files which are dime.cfg in the current directory and /etc/dime.cfg
initConfig :: IO ()
initConfig = do
  readConfigFile "dime.cfg"
  readConfigFile "/etc/dime.cfg"