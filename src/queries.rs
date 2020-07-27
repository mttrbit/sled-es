use cqrs_es::{Aggregate, AggregateError, DomainEvent, EventEnvelope, Query, QueryProcessor};
use serde::de::DeserializeOwned;
use serde::Serialize;
use sled::Db;
use std::fmt::Debug;
use std::marker::PhantomData;

type ErrorHandler = dyn Fn(AggregateError);

pub struct GenericQueryRepository<V, A, E>
where
    V: Query<A, E>,
    A: Aggregate,
    E: DomainEvent<A>,
{
    db: Db,
    query_name: String,
    error_handler: Option<Box<ErrorHandler>>,
    _phantom: PhantomData<(V, A, E)>,
}

impl<V, A, E> GenericQueryRepository<V, A, E>
where
    V: Query<A, E>,
    A: Aggregate,
    E: DomainEvent<A>,
{
    #[must_use]
    pub fn new(query_name: &str, db: Db) -> Self {
        GenericQueryRepository {
            query_name: query_name.to_string(),
            db,
            error_handler: None,
            _phantom: PhantomData,
        }
    }

    pub fn with_error_handler(&mut self, error_handler: Box<ErrorHandler>) {
        self.error_handler = Some(error_handler);
    }

    pub fn view_name(&self) -> String {
        self.query_name.to_string()
    }

    fn load_mut(&self, query_instance_id: String) -> Result<(V, QueryContext<V>), AggregateError> {
        let view_name = self.view_name();
        let tree = self.db.open_tree(view_name.into_bytes()).unwrap();
        let query_id = query_instance_id.clone();
        let result = tree.get(query_id.into_bytes()).unwrap();
        match result {
            Some(v) => {
                let view = serde_json::from_slice(v.to_vec().as_mut()).unwrap();
                let view_context = QueryContext {
                    query_name: self.view_name(),
                    query_instance_id,
                    _phantom: PhantomData,
                };
                Ok((view, view_context))
            }
            None => {
                let view_context = QueryContext {
                    query_name: self.query_name.clone(),
                    query_instance_id,
                    _phantom: PhantomData,
                };
                Ok((Default::default(), view_context))
            }
        }
    }

    pub fn apply_events(&self, query_instance_id: &str, events: &[EventEnvelope<A, E>]) {
        match self.load_mut(query_instance_id.to_string()) {
            Ok((mut view, view_context)) => {
                for event in events {
                    view.update(event);
                }
                view_context.commit(&self.db, view);
            }
            Err(e) => match &self.error_handler {
                None => {}
                Some(handler) => (handler)(e),
            },
        }
    }

    pub fn load(&self, query_instance_id: String) -> Option<V> {
        let view_name = self.view_name();
        let tree = self.db.open_tree(view_name.into_bytes()).unwrap();
        let query_id = query_instance_id.clone();
        let result = tree.get(query_id.into_bytes()).unwrap();
        match result {
            Some(v) => match serde_json::from_slice(v.to_vec().as_mut()) {
                Ok(view) => Some(view),
                Err(e) => {
                    match &self.error_handler {
                        None => {}
                        Some(handler) => (handler)(e.into()),
                    }
                    None
                }
            },
            None => None,
        }
    }
}

impl<Q, A, E> QueryProcessor<A, E> for GenericQueryRepository<Q, A, E>
where
    Q: Query<A, E>,
    E: DomainEvent<A>,
    A: Aggregate,
{
    fn dispatch(&self, query_instance_id: &str, events: &[EventEnvelope<A, E>]) {
        self.apply_events(&query_instance_id.to_string(), &events);
    }
}

struct QueryContext<V>
where
    V: Debug + Default + Serialize + DeserializeOwned + Default,
{
    query_name: String,
    query_instance_id: String,
    _phantom: PhantomData<V>,
}

impl<V> QueryContext<V>
where
    V: Debug + Default + Serialize + DeserializeOwned + Default,
{
    fn commit(&self, db: &Db, view: V) {
        // let query_instance_id = &self.query_instance_id;
        let payload = match serde_json::to_value(&view) {
            Ok(payload) => payload,
            Err(err) => {
                panic!(
                    "unable to covert view '{}' with id: '{}', to value: {}\n  view: {:?}",
                    &self.query_instance_id, &self.query_name, err, &view
                );
            }
        };
        let query_name = self.query_name.clone();
        let tree = db.open_tree(query_name.into_bytes()).unwrap();
        let key = self.query_instance_id.to_string();
        let value: &str = payload.as_str().unwrap();
        match tree.insert(key, value) {
            Ok(_) => {}
            Err(err) => {
                panic!(
                    "unable to update view '{}' with id: '{}', encountered: {}",
                    &self.query_instance_id, &self.query_name, err
                );
            }
        };
    }
}

// #[cfg(test)]
// mod queries_test {

//     #[test]
//     fn test_simple() {
//         let db = sled::open("data/db").unwrap();
//     }
// }
