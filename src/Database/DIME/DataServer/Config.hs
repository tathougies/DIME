module Database.DIME.DataServer.Config
    (module Database.DIME.Config,
     initDataServerConfig
    ) where

import Database.DIME.Config

initDataServerConfig :: IO ()
initDataServerConfig = do
  setDefaults []

  initConfig