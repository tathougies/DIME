module Database.DIME.Server (
) where

import qualified Data.Map as M
import qualified Data.Set as S

import System.Log.Logger

-- For module use only
moduleName = "Database.DIME.Server"

{-| The state of the name server. This contains the table mapping, which is serialized to disk
    periodically
 -}
data NameState = NameState {
      getTables :: M.Map String Table , -- Map of names to tables
      getPeers :: S.Set String -- Set of peers
    }

-- Methods to write the name state to disk

emptyNameState :: NameState
emptyNameState = NameState M.empty

nameServerMain :: NameState -> IO ()
nameServerMain startState = do
  infoM moduleName "DIME server starting..."
  
  return ()