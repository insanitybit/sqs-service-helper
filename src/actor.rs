pub trait Actor<T> {
    pub fn route_msg(msg: T);
}