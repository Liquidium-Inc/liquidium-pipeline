use candid::Nat;

#[allow(dead_code, clippy::wrong_self_convention)]
pub trait WadRayMath {
    fn wad_mul(&self, other: &Self) -> Self;
    fn wad_div(&self, other: &Self) -> Self;
    fn ray_mul(&self, other: &Self) -> Self;
    fn ray_div(&self, other: &Self) -> Self;
    fn ray_to_wad(&self) -> Self;
    fn wad_to_ray(&self) -> Self;
    fn ray_pow(&self, exp: u128) -> Self;
    fn to_ray(&self) -> Self;
    fn to_wad(&self) -> Self;
    fn from_ray(&self) -> Self;
    fn from_wad(&self) -> Self;
}

pub const WAD: u128 = 1_000_000_000_000_000_000; // 1e18
pub const HALF_WAD: u128 = WAD / 2;
pub const RAY: u128 = 1_000_000_000_000_000_000_000_000_000; // 1e27
pub const HALF_RAY: u128 = RAY / 2;
pub const WAD_RAY_RATIO: u128 = 1_000_000_000; // 1e9

#[allow(dead_code)]
pub fn ray_from(value: u128) -> Nat {
    Nat::from(value).to_ray()
}

impl WadRayMath for Nat {
    fn to_ray(&self) -> Self {
        self.clone() * RAY
    }

    fn to_wad(&self) -> Self {
        self.clone() * WAD
    }

    fn from_ray(&self) -> Self {
        self.clone() / RAY
    }

    fn from_wad(&self) -> Self {
        self.clone() / WAD
    }

    fn wad_mul(&self, other: &Self) -> Self {
        (Nat::from(HALF_WAD) + (self.clone() * other.clone())) / Nat::from(WAD)
    }

    fn wad_div(&self, other: &Self) -> Self {
        let half_b = other.clone() / Nat::from(2u8);
        (half_b + (self.clone() * WAD)) / other.clone()
    }

    fn ray_mul(&self, other: &Self) -> Self {
        (Nat::from(HALF_RAY) + (self.clone() * other.clone())) / Nat::from(RAY)
    }

    fn ray_div(&self, other: &Self) -> Self {
        let half_b = other.clone() / Nat::from(2u8);
        (half_b + (self.clone() * Nat::from(RAY))) / other.clone()
    }

    fn ray_to_wad(&self) -> Self {
        let half_ratio = WAD_RAY_RATIO / 2;
        (Nat::from(half_ratio) + self.clone()) / Nat::from(WAD_RAY_RATIO)
    }

    fn wad_to_ray(&self) -> Self {
        self.clone() * Nat::from(WAD_RAY_RATIO)
    }

    fn ray_pow(&self, mut exp: u128) -> Self {
        // Compute self^exp in ray precision.
        let mut z = if !exp.is_multiple_of(2) {
            self.clone()
        } else {
            Nat::from(RAY)
        };
        let mut base = self.clone();
        exp /= 2;
        while exp != 0 {
            base = base.ray_mul(&base);
            if !exp.is_multiple_of(2) {
                z = z.ray_mul(&base);
            }
            exp /= 2;
        }
        z
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use candid::Nat;

    #[test]
    fn test_wad_mul() {
        // 2.0 * 3.0 = 6.0 in wad representation.
        let two = Nat::from(2u8).to_wad();
        let three = Nat::from(3u8).to_wad();
        let product = two.wad_mul(&three);
        assert_eq!(product, Nat::from(6u8) * Nat::from(WAD));
    }

    #[test]
    fn test_wad_div() {
        // 6.0 / 3.0 = 2.0 in wad representation.
        let six = Nat::from(6u8).to_wad();
        let three = Nat::from(3u8).to_wad();
        let quotient = six.wad_div(&three);
        assert_eq!(quotient, Nat::from(2u8) * Nat::from(WAD));
    }

    #[test]
    fn test_ray_mul() {
        // 2.0 * 3.0 = 6.0 in ray representation.
        let two = Nat::from(2u8).to_ray();
        let three = Nat::from(3u8).to_ray();
        let product = two.ray_mul(&three);
        assert_eq!(product, Nat::from(6u8).to_ray());
    }

    #[test]
    fn test_ray_div() {
        // 2.0 / 3.0 in ray representation.
        let two = Nat::from(2u8).to_ray();
        let three = Nat::from(3u8).to_ray();
        let quotient = two.ray_div(&three);
        // Convert back to wad to check the value ~0.666...
        let wad_quotient = quotient.ray_to_wad();
        let expected = (Nat::from(2u8) * Nat::from(WAD)) / Nat::from(3u8);
        // Allow a difference of 1 unit
        let diff = if wad_quotient > expected {
            wad_quotient - expected
        } else {
            expected - wad_quotient
        };
        assert!(diff <= 1u8);
    }

    #[test]
    fn test_ray_to_wad_and_wad_to_ray() {
        let wad_value = Nat::from(5u8).to_wad();
        let ray_value = wad_value.wad_to_ray();
        let wad_converted = ray_value.ray_to_wad();
        assert_eq!(wad_value, wad_converted);
    }

    #[test]
    fn test_ray_pow() {
        // Calculate 2^3 = 8.0 in ray precision.
        let two = Nat::from(2u8).to_ray();
        let result = two.ray_pow(3);
        assert_eq!(result, Nat::from(8u8).to_ray());
    }
}
