#[cfg(test)]
mod tests {
    use crate::auth::authentication::*;
    use crate::auth::authorization::*;
    use crate::auth::token::*;

    #[test]
    fn test_basic_authentication() {
        let mut auth = BasicAuthenticator::new();
        auth.add_user("user1", "password1");

        assert!(auth.authenticate("user1", "password1").unwrap());
        assert!(!auth.authenticate("user1", "wrong_password").unwrap());
    }

    #[test]
    fn test_rbac() {
        let mut rbac = RoleBasedAccessControl::new();
        rbac.add_role("admin", vec![
            Permission::Read,
            Permission::Write,
            Permission::Admin,
        ]);
        rbac.add_role("user", vec![Permission::Read]);

        rbac.assign_role("user1", "admin");
        rbac.assign_role("user2", "user");

        assert!(rbac.has_permission("user1", &Permission::Admin));
        assert!(!rbac.has_permission("user2", &Permission::Admin));
    }

    #[test]
    fn test_token_management() {
        let token_manager = TokenManager::new(b"secret");
        let token = token_manager
            .generate_token("user1", vec!["admin".to_string()])
            .unwrap();

        let claims = token_manager.verify_token(&token).unwrap();
        assert_eq!(claims.sub, "user1");
        assert_eq!(claims.roles, vec!["admin"]);
    }

    #[test]
    fn test_invalid_token() {
        let token_manager = TokenManager::new(b"secret");
        let invalid_token = "invalid.token.here";

        let result = token_manager.verify_token(invalid_token);
        assert!(result.is_err());
    }

    #[test]
    fn test_permission_check() {
        let mut rbac = RoleBasedAccessControl::new();
        rbac.add_role("admin", vec![
            Permission::Read,
            Permission::Write,
            Permission::Admin,
        ]);
        rbac.add_role("user", vec![Permission::Read]);

        rbac.assign_role("user1", "admin");
        rbac.assign_role("user2", "user");

        assert!(rbac.has_permission("user1", &Permission::Read));
        assert!(rbac.has_permission("user1", &Permission::Write));
        assert!(rbac.has_permission("user1", &Permission::Admin));

        assert!(rbac.has_permission("user2", &Permission::Read));
        assert!(!rbac.has_permission("user2", &Permission::Write));
        assert!(!rbac.has_permission("user2", &Permission::Admin));
    }

    #[test]
    fn test_add_and_remove_role() {
        let mut rbac = RoleBasedAccessControl::new();
        rbac.add_role("admin", vec![
            Permission::Read,
            Permission::Write,
            Permission::Admin,
        ]);

        rbac.assign_role("user1", "admin");
        assert!(rbac.has_permission("user1", &Permission::Admin));

        rbac.remove_role("user1", "admin");
        assert!(!rbac.has_permission("user1", &Permission::Admin));
    }
}
