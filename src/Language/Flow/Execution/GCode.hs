module Language.Flow.Execution.GCode
    ( GCode(..),
      GCodeSequence,
      GCodeBuiltin,
      GCodeProgram(..)
    ) where

import qualified Data.Map as Map

import Language.Flow.Execution.Types


-- | G-Machine instructions as found in Simon Peyton-Jones's book
data GCode =
    -- State transitions (control)
    Eval |
    Unwind |
    Return |
    Jump Label |
    JFalse Label |

    -- Data manipulation
    Push StackOffset |
    PushLocation GMachineAddress |
    PushGlobal GlobalName |
    Pop StackOffset |
    Slide StackOffset |
    Update StackOffset |
    Alloc Int |
    MkAp

type GCodeSequence = [GCode]