//! This module contains the rendering functionality for the load phase.

use std::collections::BTreeMap;

use minijinja::Value;
use serde::Serialize;

use crate::{functions::Var, phases::load::secret_renderer::secret_context_env_var};

pub mod init;
pub mod secret_renderer;

/// A struct that contains the context for the deps phase.
#[derive(Serialize)]
pub struct LoadContext {
    env_var: Value,
    var: Value,
    target: Value,
}

impl LoadContext {
    /// Create a new DepsContext.
    pub fn new(vars: BTreeMap<String, dbt_yaml::Value>) -> Self {
        Self {
            env_var: Value::from_func_func("env_var", secret_context_env_var),
            var: Value::from_object(Var::new(vars)),
            target: Value::from_serialize(BTreeMap::<String, Value>::new()),
        }
    }

    /// The context for rendering a *dependency's* `packages.yml`.
    ///
    /// A just-installed package's own `packages.yml` is rendered and then
    /// installed from, so with the root context it could read any environment
    /// variable of the machine running `dbt deps` (and route it into a
    /// `local:` path, a `git:` revision or a `tarball:` URL). Only the root
    /// project's `packages.yml` is the user's own; a dependency's gets no
    /// `env_var`.
    pub fn restricted(vars: BTreeMap<String, dbt_yaml::Value>) -> Self {
        Self {
            env_var: Value::from_func_func("env_var", |_state, _args| {
                Err(minijinja::Error::new(
                    minijinja::ErrorKind::InvalidOperation,
                    "env_var is not available when rendering a dependency's packages.yml; \
                     only the root project's packages.yml may read environment variables",
                ))
            }),
            var: Value::from_object(Var::new(vars)),
            target: Value::from_serialize(BTreeMap::<String, Value>::new()),
        }
    }
}
