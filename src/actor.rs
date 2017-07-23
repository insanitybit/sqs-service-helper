pub trait Actor<T> {
    fn route_msg(msg: T);
}