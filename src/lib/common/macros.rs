#[macro_export]
macro_rules! RcRefCell {
    ($a:expr) => {
        Rc::new(RefCell::new($a))
    };
}
