use ic_agent::{
    Identity,
    identity::{BasicIdentity, Secp256k1Identity},
};

pub fn create_identity_from_pem_file(pem_file: &str) -> Result<Box<dyn Identity>, String> {
    match BasicIdentity::from_pem_file(pem_file) {
        Ok(basic_identity) => Ok(Box::new(basic_identity)),
        Err(_) => match Secp256k1Identity::from_pem_file(pem_file) {
            Ok(secp256k1_identity) => Ok(Box::new(secp256k1_identity)),
            Err(err) => Err(format!(
                "Failed to create identity from pem file at {}. Unknown identity format. {}",
                pem_file,
                err.to_string()
            )),
        },
    }
}
