/// Creates an Rc<RefCell<Value>>
///
/// Equivalent to Rc::new( RefCell::new( value ) )
#[macro_export]
macro_rules! RcRefCell {
    ($a:expr) => {
        std::rc::Rc::new(std::cell::RefCell::new($a))
    };
}
