{-# LANGUAGE NamedFieldPuns #-}

-- | cabal-install CLI command: repl
--
module Distribution.Client.CmdRepl (
    replCommand,
    replAction,
  ) where

import Distribution.Client.ProjectOrchestration
import Distribution.Client.ProjectConfig
         ( BuildTimeSettings(..) )
import Distribution.Client.ProjectPlanning
         ( PackageTarget(..) )
import Distribution.Client.BuildTarget
         ( readUserBuildTargets )

import Distribution.Client.Setup
         ( GlobalFlags, ConfigFlags(..), ConfigExFlags, InstallFlags )
import Distribution.Simple.Setup
         ( HaddockFlags, fromFlagOrDefault )
import Distribution.Verbosity
         ( normal )

import Control.Monad (when)

import Distribution.Simple.Command
         ( CommandUI(..), usageAlternatives )
import Distribution.Simple.Utils
         ( wrapText, die )
import qualified Distribution.Client.Setup as Client

replCommand :: CommandUI (ConfigFlags, ConfigExFlags, InstallFlags, HaddockFlags)
replCommand = Client.installCommand {
  commandName         = "new-repl",
  commandSynopsis     = "Open an interactive session for the given component.",
  commandUsage        = usageAlternatives "new-repl" [ "[TARGET] [FLAGS]" ],
  commandDescription  = Just $ \_ -> wrapText $
        "Open an interactive session for a component within the project. The "
     ++ "available targets are the same as for the 'new-build' command: "
     ++ "individual components within packages in the project, including "
     ++ "libraries, executables, test-suites or benchmarks. Packages can "
     ++ "also be specified in which case the library component in the "
     ++ "package will be used, or the (first listed) executable in the "
     ++ "package if there is no library.\n\n"

     ++ "Dependencies are built or rebuilt as necessary. Additional "
     ++ "configuration flags can be specified on the command line and these "
     ++ "extend the project configuration from the 'cabal.project', "
     ++ "'cabal.project.local' and other files.",
  commandNotes        = Just $ \pname ->
        "Examples, open an interactive session:\n"
     ++ "  " ++ pname ++ " new-repl\n"
     ++ "    for the default component in the package in the current directory\n"
     ++ "  " ++ pname ++ " new-repl pkgname\n"
     ++ "    for the default component in the package named 'pkgname'\n"
     ++ "  " ++ pname ++ " new-repl ./pkgfoo\n"
     ++ "    for the default component in the package in the ./pkgfoo directory\n"
     ++ "  " ++ pname ++ " new-repl cname\n"
     ++ "    for the component named 'cname'\n"
     ++ "  " ++ pname ++ " new-repl pkgname:cname\n"
     ++ "    for the component 'cname' in the package 'pkgname'\n\n"


     ++ "Note: this command is part of the new project-based system (aka "
     ++ "nix-style\nlocal builds). These features are currently in beta. "
     ++ "Please see\n"
     ++ "http://cabal.readthedocs.io/en/latest/nix-local-build-overview.html "
     ++ "for\ndetails and advice on what you can expect to work. If you "
     ++ "encounter problems\nplease file issues at "
     ++ "https://github.com/haskell/cabal/issues and if you\nhave any time "
     ++ "to get involved and help with testing, fixing bugs etc then\nthat "
     ++ "is very much appreciated.\n"
   }

-- | The @repl@ command is very much like @build@. It brings the install plan
-- up to date, selects that part of the plan needed by the given or implicit
-- repl target and then executes the plan.
--
-- Compared to @build@ the difference is that only one target is allowed
-- (given or implicit) and the target type is repl rather than build. The
-- general plan execution infrastructure handles both build and repl targets.
--
-- For more details on how this works, see the module
-- "Distribution.Client.ProjectOrchestration"
--
replAction :: (ConfigFlags, ConfigExFlags, InstallFlags, HaddockFlags)
              -> [String] -> GlobalFlags -> IO ()
replAction (configFlags, configExFlags, installFlags, haddockFlags)
           targetStrings globalFlags = do

    userTargets <- readUserBuildTargets targetStrings

    buildCtx <-
      runProjectPreBuildPhase
        verbosity
        ( globalFlags, configFlags, configExFlags
        , installFlags, haddockFlags )
        PreBuildHooks {
          hookPrePlanning      = \_ _ _ -> return (),

          hookSelectPlanSubset = \buildSettings elaboratedPlan -> do
            when (buildSettingOnlyDeps buildSettings) $
              die $ "The repl command does not support '--only-dependencies'. "
                 ++ "You may wish to use 'build --only-dependencies' and then "
                 ++ "use 'repl'."
            -- Interpret the targets on the command line as repl targets
            -- (as opposed to say build or haddock targets).
            selectTargets
              verbosity
              ReplDefaultComponent
              ReplSpecificComponent
              userTargets
              False -- onlyDependencies, always False for repl
              elaboratedPlan
            --TODO: [required eventually] reject multiple targets, or at least
            -- targets spanning multiple components. ie it's ok to have two
            -- module/file targets in the same component, but not two that live
            -- in different components.
        }

    printPlan verbosity buildCtx

    buildOutcomes <- runProjectBuildPhase verbosity buildCtx
    runProjectPostBuildPhase verbosity buildCtx buildOutcomes
  where
    verbosity = fromFlagOrDefault normal (configVerbosity configFlags)

