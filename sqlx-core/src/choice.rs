#![allow(unused)]

pub enum Choice<'t, T: 'static> {
    Owned(T),
    Borrowed(&'t T),
}

impl<T: Clone> Choice<'_, T> {
    fn into_owned(self) -> T {
        match self {
            Choice::Owned(t) => t,
            Choice::Borrowed(t) => t.clone(),
        }
    }
}

impl<T> From<T> for Choice<'_, T> {
    fn from(t: T) -> Self {
        Choice::Owned(t)
    }
}

impl<'t, T> From<&'t T> for Choice<'t, T> {
    fn from(t: &'t T) -> Self {
        Choice::Borrowed(t)
    }
}

pub trait IntoCloned<T, C>
where
    T: for<'c> Into<Choice<'c, C>>,
{
    fn into_cloned(self) -> C;
}

pub trait IntoCloned2<C> {
    fn into_cloned2(self) -> C;
}

impl<T, C> IntoCloned2<C> for T
where
    T: for<'c> Into<Choice<'c, C>>,
    C: Clone + 'static,
{
    fn into_cloned2(self) -> C {
        let choice = self.into();
        choice.into_owned()
    }
}
