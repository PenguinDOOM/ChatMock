use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Mutex;

pub trait RetainedUpstreamWebsocket: Clone {
    fn close_socket(&self);
    fn is_socket_closed(&self) -> bool;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedUpstreamWebsocketLease<T> {
    pub response_id: Option<String>,
    pub upstream_ws: T,
    pub created: bool,
    lease_id: u64,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error(
    "A stateful HTTP websocket bridge request for response '{response_id}' is already in progress."
)]
pub struct ResponsesWebsocketSessionConflictError {
    pub response_id: String,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("Too many retained upstream websocket sessions are active right now.")]
pub struct ResponsesWebsocketSessionCapacityError;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("No retained upstream websocket exists for response '{response_id}'.")]
pub struct ResponsesWebsocketSessionNotFoundError {
    pub response_id: String,
}

#[derive(Debug)]
struct RetainedUpstreamWebsocketEntry<T> {
    lease_id: u64,
    upstream_ws: T,
    in_use: bool,
    last_used: u64,
}

#[derive(Debug)]
struct RegistryState<T> {
    max_sessions: usize,
    next_lease_id: u64,
    tick: u64,
    sessions: HashMap<String, RetainedUpstreamWebsocketEntry<T>>,
    anonymous_leases: HashMap<u64, T>,
    pending_reservations: HashSet<u64>,
}

#[derive(Debug)]
pub struct RetainedUpstreamWebsocketRegistry<T> {
    state: Mutex<RegistryState<T>>,
}

impl<T> RegistryState<T> {
    fn new(max_sessions: usize) -> Self {
        Self {
            max_sessions: max_sessions.max(1),
            next_lease_id: 1,
            tick: 0,
            sessions: HashMap::new(),
            anonymous_leases: HashMap::new(),
            pending_reservations: HashSet::new(),
        }
    }

    fn advance_tick(&mut self) -> u64 {
        self.tick = self.tick.saturating_add(1);
        self.tick
    }

    fn next_lease_id(&mut self) -> u64 {
        let lease_id = self.next_lease_id;
        self.next_lease_id = self.next_lease_id.saturating_add(1);
        lease_id
    }

    fn active_session_count(&self) -> usize {
        self.sessions.len() + self.anonymous_leases.len() + self.pending_reservations.len()
    }
}

impl<T> RetainedUpstreamWebsocketRegistry<T>
where
    T: RetainedUpstreamWebsocket,
{
    pub fn new(max_sessions: usize) -> Self {
        Self {
            state: Mutex::new(RegistryState::new(max_sessions)),
        }
    }

    fn normalize_response_id(response_id: Option<&str>) -> Option<String> {
        response_id
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    }

    fn close_entry(state: &mut RegistryState<T>, response_id: &str) {
        if let Some(entry) = state.sessions.remove(response_id) {
            entry.upstream_ws.close_socket();
        }
    }

    fn evict_closed_entries(state: &mut RegistryState<T>) {
        let closed_ids = state
            .sessions
            .iter()
            .filter(|(_, entry)| !entry.in_use && entry.upstream_ws.is_socket_closed())
            .map(|(response_id, _)| response_id.clone())
            .collect::<Vec<_>>();
        for response_id in closed_ids {
            Self::close_entry(state, &response_id);
        }
    }

    fn evict_to_capacity(
        state: &mut RegistryState<T>,
    ) -> Result<(), ResponsesWebsocketSessionCapacityError> {
        Self::evict_closed_entries(state);
        while state.active_session_count() >= state.max_sessions {
            let idle_response_id = state
                .sessions
                .iter()
                .filter(|(_, entry)| !entry.in_use)
                .min_by_key(|(_, entry)| entry.last_used)
                .map(|(response_id, _)| response_id.clone());
            let Some(response_id) = idle_response_id else {
                return Err(ResponsesWebsocketSessionCapacityError);
            };
            Self::close_entry(state, &response_id);
        }
        Ok(())
    }

    pub fn acquire<F>(
        &self,
        response_id: Option<&str>,
        create_upstream_websocket: F,
    ) -> Result<RetainedUpstreamWebsocketLease<T>, Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnOnce() -> Result<T, Box<dyn std::error::Error + Send + Sync>>,
    {
        let normalized_response_id = Self::normalize_response_id(response_id);

        let reservation = {
            let mut state = self.state.lock().expect("registry lock poisoned");
            if let Some(response_id) = normalized_response_id.as_deref() {
                if let Some(entry) = state.sessions.get(response_id) {
                    if entry.in_use {
                        return Err(Box::new(ResponsesWebsocketSessionConflictError {
                            response_id: response_id.to_string(),
                        }));
                    }
                    if entry.upstream_ws.is_socket_closed() {
                        Self::close_entry(&mut state, response_id);
                        return Err(Box::new(ResponsesWebsocketSessionNotFoundError {
                            response_id: response_id.to_string(),
                        }));
                    }
                } else {
                    return Err(Box::new(ResponsesWebsocketSessionNotFoundError {
                        response_id: response_id.to_string(),
                    }));
                }

                let tick = state.advance_tick();
                let entry = state
                    .sessions
                    .get_mut(response_id)
                    .expect("checked retained websocket entry");
                entry.in_use = true;
                entry.last_used = tick;
                return Ok(RetainedUpstreamWebsocketLease {
                    response_id: Some(response_id.to_string()),
                    upstream_ws: entry.upstream_ws.clone(),
                    created: false,
                    lease_id: entry.lease_id,
                });
            }

            Self::evict_to_capacity(&mut state)?;
            let reservation = state.next_lease_id();
            state.pending_reservations.insert(reservation);
            reservation
        };

        let upstream_ws = match create_upstream_websocket() {
            Ok(upstream_ws) => upstream_ws,
            Err(error) => {
                self.state
                    .lock()
                    .expect("registry lock poisoned")
                    .pending_reservations
                    .remove(&reservation);
                return Err(error);
            }
        };

        let mut state = self.state.lock().expect("registry lock poisoned");
        state.pending_reservations.remove(&reservation);
        state
            .anonymous_leases
            .insert(reservation, upstream_ws.clone());
        Ok(RetainedUpstreamWebsocketLease {
            response_id: None,
            upstream_ws,
            created: true,
            lease_id: reservation,
        })
    }

    pub async fn acquire_async<F, Fut>(
        &self,
        response_id: Option<&str>,
        create_upstream_websocket: F,
    ) -> Result<RetainedUpstreamWebsocketLease<T>, Box<dyn std::error::Error + Send + Sync>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>>,
    {
        let normalized_response_id = Self::normalize_response_id(response_id);

        let reservation = {
            let mut state = self.state.lock().expect("registry lock poisoned");
            if let Some(response_id) = normalized_response_id.as_deref() {
                if let Some(entry) = state.sessions.get(response_id) {
                    if entry.in_use {
                        return Err(Box::new(ResponsesWebsocketSessionConflictError {
                            response_id: response_id.to_string(),
                        }));
                    }
                    if entry.upstream_ws.is_socket_closed() {
                        Self::close_entry(&mut state, response_id);
                        return Err(Box::new(ResponsesWebsocketSessionNotFoundError {
                            response_id: response_id.to_string(),
                        }));
                    }
                } else {
                    return Err(Box::new(ResponsesWebsocketSessionNotFoundError {
                        response_id: response_id.to_string(),
                    }));
                }

                let tick = state.advance_tick();
                let entry = state
                    .sessions
                    .get_mut(response_id)
                    .expect("checked retained websocket entry");
                entry.in_use = true;
                entry.last_used = tick;
                return Ok(RetainedUpstreamWebsocketLease {
                    response_id: Some(response_id.to_string()),
                    upstream_ws: entry.upstream_ws.clone(),
                    created: false,
                    lease_id: entry.lease_id,
                });
            }

            Self::evict_to_capacity(&mut state)?;
            let reservation = state.next_lease_id();
            state.pending_reservations.insert(reservation);
            reservation
        };

        let upstream_ws = match create_upstream_websocket().await {
            Ok(upstream_ws) => upstream_ws,
            Err(error) => {
                self.state
                    .lock()
                    .expect("registry lock poisoned")
                    .pending_reservations
                    .remove(&reservation);
                return Err(error);
            }
        };

        let mut state = self.state.lock().expect("registry lock poisoned");
        state.pending_reservations.remove(&reservation);
        state
            .anonymous_leases
            .insert(reservation, upstream_ws.clone());
        Ok(RetainedUpstreamWebsocketLease {
            response_id: None,
            upstream_ws,
            created: true,
            lease_id: reservation,
        })
    }

    pub fn release(
        &self,
        lease: RetainedUpstreamWebsocketLease<T>,
        retain: bool,
        response_id: Option<&str>,
    ) {
        let retained_response_id =
            Self::normalize_response_id(response_id).or_else(|| lease.response_id.clone());
        let mut state = self.state.lock().expect("registry lock poisoned");

        if let Some(upstream_ws) = state.anonymous_leases.remove(&lease.lease_id) {
            if retain {
                if let Some(response_id) = retained_response_id {
                    if !upstream_ws.is_socket_closed() {
                        let last_used = state.advance_tick();
                        state.sessions.insert(
                            response_id,
                            RetainedUpstreamWebsocketEntry {
                                lease_id: lease.lease_id,
                                upstream_ws,
                                in_use: false,
                                last_used,
                            },
                        );
                        return;
                    }
                }
            }
            upstream_ws.close_socket();
            return;
        }

        let Some(original_response_id) = lease.response_id.clone() else {
            if !retain {
                lease.upstream_ws.close_socket();
            }
            return;
        };

        let Some(entry) = state.sessions.remove(&original_response_id) else {
            if !retain {
                lease.upstream_ws.close_socket();
            }
            return;
        };

        if entry.lease_id != lease.lease_id {
            state.sessions.insert(original_response_id, entry);
            if !retain {
                lease.upstream_ws.close_socket();
            }
            return;
        }

        if retain {
            if let Some(response_id) = retained_response_id {
                if !entry.upstream_ws.is_socket_closed() {
                    let last_used = state.advance_tick();
                    state.sessions.insert(
                        response_id,
                        RetainedUpstreamWebsocketEntry {
                            lease_id: entry.lease_id,
                            upstream_ws: entry.upstream_ws,
                            in_use: false,
                            last_used,
                        },
                    );
                    return;
                }
            }
        }

        entry.upstream_ws.close_socket();
    }
}
