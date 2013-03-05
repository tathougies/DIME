module Database.DIME.Server.Config
    (module Database.DIME.Config,
     initServerConfig
    ) where

import Database.DIME.Config

initServerConfig :: IO ()
initServerConfig = do
  setDefaults []

  initConfig