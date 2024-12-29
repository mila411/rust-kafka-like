use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum Permission {
    Read,
    Write,
    Admin,
}

pub struct RoleBasedAccessControl {
    roles: HashMap<String, Vec<Permission>>,
    user_roles: HashMap<String, Vec<String>>,
}

impl RoleBasedAccessControl {
    pub fn new() -> Self {
        Self {
            roles: HashMap::new(),
            user_roles: HashMap::new(),
        }
    }

    pub fn add_role(&mut self, role: &str, permissions: Vec<Permission>) {
        self.roles.insert(role.to_string(), permissions);
    }

    pub fn assign_role(&mut self, username: &str, role: &str) {
        self.user_roles
            .entry(username.to_string())
            .or_insert_with(Vec::new)
            .push(role.to_string());
    }

    pub fn remove_role(&mut self, username: &str, role: &str) {
        if let Some(roles) = self.user_roles.get_mut(username) {
            roles.retain(|r| r != role);
        }
    }

    pub fn has_permission(&self, username: &str, required_permission: &Permission) -> bool {
        self.user_roles.get(username).map_or(false, |roles| {
            roles.iter().any(|role| {
                self.roles
                    .get(role)
                    .map(|permissions| permissions.contains(required_permission))
                    .unwrap_or(false)
            })
        })
    }
}
