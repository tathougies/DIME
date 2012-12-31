module Database.DIME.Memory.State (
) where

import qualified Data.Map as M
import Data.Word

import Database.DIME
import Database.DIME.Memory.Block

import Control.Monad.State

data GenericBlock = StringBlock String
                  deriving Show

-- IntBlock (Block Int) |
                    -- StringBlock (Block String) |
                    -- DoubleBlock (Block Double)

data BlocksState =
    BlocksState {
      bsBlockMap :: M.Map BlockID GenericBlock
    }

type BlockTransform = State BlocksState

empty = BlocksState (M.empty)

transformBlocksState :: BlocksState -> BlockTransform a  -> BlocksState
transformBlocksState state transform = snd $ runState transform state

addBlock :: GenericBlock -> BlockID -> BlockTransform ()
addBlock block blockID = do
  state <-  get
  put $ state { bsBlockMap = M.insert blockID block $ bsBlockMap state }

deleteBlock :: BlockID -> BlockTransform ()
deleteBlock blockID = do
  state <- get
  put $ state { bsBlockMap = M.delete blockID $ bsBlockMap state}