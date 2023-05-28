use std::{
    any::{Any, TypeId},
    ops::Deref,
};

use fnv::FnvHashMap;

#[derive(Default)]
pub struct TypeMap(FnvHashMap<TypeId, Box<dyn Any + Sync + Send>>);

impl Deref for TypeMap {
    type Target = FnvHashMap<TypeId, Box<dyn Any + Sync + Send>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TypeMap {
    pub fn new() -> Self {
        Self(FnvHashMap::default())
    }
    
    pub fn insert<D: Any + Send + Sync>(&mut self, data: D) {
        self.0.insert(TypeId::of::<D>(), Box::new(data));
    }

    pub fn get<D: Any + Send + Sync>(&self) -> Option<&D> {
        self.0
            .get(&TypeId::of::<D>())
            .and_then(|x| x.downcast_ref::<D>())
    }
}

impl std::fmt::Debug for TypeMap {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_tuple("TypeMap").finish()
    }
}
