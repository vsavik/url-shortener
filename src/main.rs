//! ## Task Description
//!
//! The goal is to develop a backend service for shortening URLs using CQRS
//! (Command Query Responsibility Segregation) and ES (Event Sourcing)
//! approaches. The service should support the following features:
//!
//! ## Functional Requirements
//!
//! ### Creating a short link with a random slug
//!
//! The user sends a long URL, and the service returns a shortened URL with a
//! random slug.
//!
//! ### Creating a short link with a predefined slug
//!
//! The user sends a long URL along with a predefined slug, and the service
//! checks if the slug is unique. If it is unique, the service creates the short
//! link.
//!
//! ### Counting the number of redirects for the link
//!
//! - Every time a user accesses the short link, the click count should
//!   increment.
//! - The click count can be retrieved via an API.
//!
//! ### CQRS+ES Architecture
//!
//! CQRS: Commands (creating links, updating click count) are separated from
//! queries (retrieving link information).
//!
//! Event Sourcing: All state changes (link creation, click count update) must be
//! recorded as events, which can be replayed to reconstruct the system's state.
//!
//! ### Technical Requirements
//!
//! - The service must be built using CQRS and Event Sourcing approaches.
//! - The service must be possible to run in Rust Playground (so no database like
//!   Postgres is allowed)
//! - Public API already written for this task must not be changed (any change to
//!   the public API items must be considered as breaking change).

#![allow(unused_variables, dead_code)]

/// All possible errors of the [`UrlShortenerService`].
#[derive(Debug, PartialEq)]
pub enum ShortenerError {
    /// This error occurs when an invalid [`Url`] is provided for shortening.
    InvalidUrl,

    /// This error occurs when an attempt is made to use a slug (custom alias)
    /// that already exists.
    SlugAlreadyInUse,

    /// This error occurs when the provided [`Slug`] does not map to any existing
    /// short link.
    SlugNotFound,
}

/// A unique string (or alias) that represents the shortened version of the
/// URL.
#[derive(Clone, Debug, PartialEq)]
pub struct Slug(pub String);

/// The original URL that the short link points to.
#[derive(Clone, Debug, PartialEq)]
pub struct Url(pub String);

/// Shortened URL representation.
#[derive(Debug, Clone, PartialEq)]
pub struct ShortLink {
    /// A unique string (or alias) that represents the shortened version of the
    /// URL.
    pub slug: Slug,

    /// The original URL that the short link points to.
    pub url: Url,
}

/// Statistics of the [`ShortLink`].
#[derive(Debug, Clone, PartialEq)]
pub struct Stats {
    /// [`ShortLink`] to which this [`Stats`] are related.
    pub link: ShortLink,

    /// Count of redirects of the [`ShortLink`].
    pub redirects: u64,
}

/// Commands for CQRS.
pub mod commands {
    use super::{ShortLink, ShortenerError, Slug, Url};

    /// Trait for command handlers.
    pub trait CommandHandler {
        /// Creates a new short link. It accepts the original url and an
        /// optional [`Slug`]. If a [`Slug`] is not provided, the service will generate
        /// one. Returns the newly created [`ShortLink`].
        ///
        /// ## Errors
        ///
        /// See [`ShortenerError`].
        fn handle_create_short_link(
            &mut self,
            url: Url,
            slug: Option<Slug>,
        ) -> Result<ShortLink, ShortenerError>;

        /// Processes a redirection by [`Slug`], returning the associated
        /// [`ShortLink`] or a [`ShortenerError`].
        fn handle_redirect(
            &mut self,
            slug: Slug,
        ) -> Result<ShortLink, ShortenerError>;
    }
}

/// Queries for CQRS
pub mod queries {
    use super::{ShortenerError, Slug, Stats};

    /// Trait for query handlers.
    pub trait QueryHandler {
        /// Returns the [`Stats`] for a specific [`ShortLink`], such as the
        /// number of redirects (clicks).
        ///
        /// [`ShortLink`]: super::ShortLink
        fn get_stats(&self, slug: Slug) -> Result<Stats, ShortenerError>;
    }
}

use std::collections::HashMap;
use std::fmt::Debug;
use events::{Event, EventType};

/// CQRS and Event Sourcing-based service implementation
pub struct UrlShortenerService {
    events: HashMap<String, Vec<Event>>,
    stats: HashMap<String, Stats>
}

impl UrlShortenerService {
    /// Creates a new instance of the service
    pub fn new() -> Self {
        Self {
            events: HashMap::new(),
            stats: HashMap::new()
        }
    }
}

use domain::ShortLinkAggregate as ShortLinkAggregate;

impl commands::CommandHandler for UrlShortenerService {
    fn handle_create_short_link(
        &mut self,
        url: Url,
        slug: Option<Slug>,
    ) -> Result<ShortLink, ShortenerError> {
        let mut aggregate = ShortLinkAggregate::new(self);

        match slug {
            Some(slug) => aggregate.rehydrate_by_slug(&slug),
            None => aggregate.create_random_slug()
        };

        let short_link = aggregate.create_short_link(&url)?;

        Ok(short_link)
    }

    fn handle_redirect(
        &mut self,
        slug: Slug,
    ) -> Result<ShortLink, ShortenerError> {
        let mut aggregate = ShortLinkAggregate::new(self);
        aggregate.rehydrate_by_slug(&slug);
        let short_link = aggregate.redirect()?;

        Ok(short_link)
    }
}

impl queries::QueryHandler for UrlShortenerService {
    fn get_stats(&self, slug: Slug) -> Result<Stats, ShortenerError> {
        let stats_result = self.stats.get(&slug.0);
        match stats_result {
            Some(stats) => { Ok(stats.clone()) }
            None => { Err(ShortenerError::SlugNotFound) }
        }
    }
}

mod events {
    use super::{Slug, Url};

    #[derive(Clone, Debug, PartialEq)]
    pub struct Event {
        pub slug: Slug,
        pub event_type: EventType
    }

    #[derive(Clone, Debug, PartialEq)]
    pub enum EventType {
        ShortLinkCreated(Url),
        ShortLinkRedirected
    }
}

impl domain::EventBroker for UrlShortenerService {
    fn publish_event(&mut self, event: &Event) {
        // Save event to event store
        self.events.entry(event.slug.0.clone()).or_default().push(event.clone());

        // Update Query Model
        match &event.event_type {
            EventType::ShortLinkCreated(url) => {
                let stats = Stats {
                    link: ShortLink { slug: event.slug.clone(), url: url.clone() },
                    redirects: 0
                };

                self.stats.insert(event.slug.0.clone(), stats);
            }
            EventType::ShortLinkRedirected => {
                if let Some(stats) = self.stats.get_mut(&event.slug.0) {
                    stats.redirects += 1;
                }
            }
        }
    }

    fn iter_by_slug(&self, slug: &Slug) -> Vec<Event> {
        if let Some(events) = self.events.get(&slug.0) {
            events.clone()
        } else {
            Vec::new()
        }
    }
}

mod domain {
    use std::time::SystemTime;
    use super::events::{Event, EventType};
    use super::{ShortLink, ShortenerError, Slug, Url};

    pub trait EventBroker {
        fn publish_event(&mut self, event: &Event);

        fn iter_by_slug(&self, slug: &Slug) -> Vec<Event>;
    }

    pub struct ShortLinkAggregate<'a> {
        broker: &'a mut dyn EventBroker,
        state: ShortLink
    }

    impl<'a> ShortLinkAggregate<'a> {
        pub fn new(eb: &'a mut dyn EventBroker) -> Self {
            Self {
                broker: eb,
                state: ShortLink {
                    slug: Slug("".to_string()),
                    url: Url("".to_string())
                }
            }
        }

        pub fn rehydrate_by_slug(&mut self, slug: &Slug) {
            self.state.slug = slug.clone();
            for event in self.broker.iter_by_slug(slug) {
                self.apply_event(&event);
            }
        }

        pub fn create_random_slug(&mut self) {
            self.state.slug = generate_random_slug();
        }

        pub fn apply_event(&mut self, event: &Event) {
            self.broker.publish_event(&event);

            match &event.event_type {
                EventType::ShortLinkCreated(url) => {
                    self.state.slug = event.slug.clone();
                    self.state.url = url.clone();
                }
                _ => {}
            }
        }

        pub fn create_short_link(&mut self, url: &Url) -> Result<ShortLink, ShortenerError> {
            if !self.state.url.0.is_empty() {
                return Err(ShortenerError::SlugAlreadyInUse);
            }

            if !is_valid_url(url) {
                return Err(ShortenerError::InvalidUrl);
            }

            let event = Event {
                slug: self.state.slug.clone(),
                event_type: EventType::ShortLinkCreated(url.clone())
            };

            self.apply_event(&event);

            Ok(self.state.clone())
        }

        pub fn redirect(&mut self) -> Result<ShortLink, ShortenerError> {
            if self.state.url.0.is_empty(){
                return Err(ShortenerError::SlugNotFound)
            }

            let event = Event {
                slug: self.state.slug.clone(),
                event_type: EventType::ShortLinkRedirected
            };

            self.apply_event(&event);

            Ok(self.state.clone())
        }
    }

    /// Use external crates to generate better slug
    fn generate_random_slug() -> Slug {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
            .to_string();

        let mut str = "rand".to_string();
        str.push_str(&now);

        Slug(str)
    }

    /// This is simple implementation to avoid external dependencies.
    /// In production use "url" package instead
    fn is_valid_url(url: &Url) -> bool {
        !url.0.is_empty() && url.0.contains('.') &&
            (url.0.starts_with("http://") || url.0.starts_with("https://"))
    }
}

impl From<&str> for Slug {
    fn from(value: &str) -> Self {
        Slug(value.to_string())
    }
}

impl From<&str> for Url {
    fn from(value: &str) -> Self {
        Url(value.to_string())
    }
}

trait Print {
    fn print(&self);
}

impl<T: Debug> Print for T {
    fn print(&self) {
        println!("{:?}", self);
    }
}

fn main() {
    const SLUG_GOOGLE_VALID: &str = "goog";
    const SLUG_MISSING: &str = "missing";
    const URL_GOOGLE_VALID: &str = "https://google.com";
    const URL_INVALID: &str = "invalid-url";

    let mut service = UrlShortenerService::new();

    let command_handler: &mut dyn commands::CommandHandler = &mut service;

    println!("Create correct short link:");
    let url = Url::from(URL_GOOGLE_VALID);
    let slug = Slug::from(SLUG_GOOGLE_VALID);
    command_handler.handle_create_short_link(url, Some(slug)).print();
    println!();

    println!("Try to create duplicate slug:");
    let url = Url::from(URL_GOOGLE_VALID);
    let slug = Slug::from(SLUG_GOOGLE_VALID);
    command_handler.handle_create_short_link(url, Some(slug)).print();
    println!();

    println!("Try to create invalid URL:");
    let url = Url::from(URL_INVALID);
    command_handler.handle_create_short_link(url, None).print();
    println!();

    println!("Try to create with random slug:");
    let url = Url::from(URL_GOOGLE_VALID);
    command_handler.handle_create_short_link(url, None).print();
    println!();

    println!("Try to redirect for valid slug:");
    let slug = Slug::from(SLUG_GOOGLE_VALID);
    command_handler.handle_redirect(slug).print();
    println!();

    println!("Do the same again to increase counter to 2:");
    let slug = Slug::from(SLUG_GOOGLE_VALID);
    command_handler.handle_redirect(slug).print();
    println!();

    println!("Try to redirect missing slug:");
    let slug = Slug::from(SLUG_MISSING);
    command_handler.handle_redirect(slug).print();
    println!();

    let query_handler: &dyn queries::QueryHandler = &service;

    println!("Query existing slug:");
    let slug = Slug::from(SLUG_GOOGLE_VALID);
    query_handler.get_stats(slug).print();
    println!();

    println!("Query missing slug:");
    let slug = Slug::from(SLUG_MISSING);
    query_handler.get_stats(slug).print();
    println!();
}
