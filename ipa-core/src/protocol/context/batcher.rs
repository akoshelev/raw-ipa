use std::{collections::VecDeque, sync::{atomic::{AtomicUsize, Ordering}, Arc}};

use tokio::sync::Notify;

use crate::protocol::RecordId;

pub enum Either<L, R> {
    Left(L),
    Right(R),
}

impl<L, R> Either<L, R> {
    fn left(value: L) -> Self {
        Self::Left(value)
    }

    fn right(value: R) -> Self {
        Self::Right(value)
    }
}

#[derive(Debug)]
pub struct BatchState<B> {
    pub batch: B,
    pub notify: Arc<Notify>,
    pub first_record: RecordId,
    pub records_per_batch: usize,
    pub records: AtomicUsize,
}

pub(super) struct Batcher<'a, B> {
    batches: VecDeque<BatchState<B>>,
    first_batch: usize,
    records_per_batch: usize,
    batch_constructor: Box<dyn Fn() -> B + Send + 'a>,
}

impl<'a, B> Batcher<'a, B> {
    pub fn new(records_per_batch: usize, batch_constructor: Box<dyn Fn() -> B + Send + 'a>) -> Self {
        Self {
            batches: VecDeque::new(),
            first_batch: 0,
            records_per_batch,
            batch_constructor
        }
    }

    fn batch_offset(&self, record_id: RecordId) -> usize {
        let batch_idx = usize::from(record_id) / self.records_per_batch;
        let Some(batch_offset) = batch_idx.checked_sub(self.first_batch) else {
            panic!(
                "Batches should be processed in order. Attempting to retrieve batch {batch_idx}. \
                 The oldest active batch is batch {}.", self.first_batch,
            )
        };
        batch_offset
    }

    fn get_batch_by_offset(&mut self, batch_offset: usize) -> &mut BatchState<B> {
        if self.batches.len() <= batch_offset {
            self.batches.reserve(batch_offset - self.batches.len() + 1);
            while self.batches.len() <= batch_offset {
                let first_record = RecordId::from(
                    (self.first_batch + self.batches.len()) * self.records_per_batch
                );
                let state = BatchState {
                    batch: (self.batch_constructor)(),
                    notify: Arc::new(Notify::new()),
                    first_record,
                    records_per_batch: self.records_per_batch,
                    records: AtomicUsize::new(0),
                };
                self.batches.push_back(state);
            }
        }
        //tracing::info!("offset {} first {} len {}", batch_offset, self.first_batch, self.batches.len());
        &mut self.batches[batch_offset]
    }

    pub fn get_batch(&mut self, record_id: RecordId) -> &mut BatchState<B> {
        self.get_batch_by_offset(self.batch_offset(record_id))
    }

    pub fn validate_record(&mut self, record_id: RecordId) -> Either<(usize, BatchState<B>), Arc<Notify>> {
        tracing::trace!("validate record {record_id}");
        let batch_offset = self.batch_offset(record_id);
        let batch = self.get_batch_by_offset(batch_offset);
        let prev_records = batch.records.fetch_add(1, Ordering::Relaxed);
        if prev_records == batch.records_per_batch - 1 {
            // I am not sure if this is okay, or if we need to tolerate batch validation requests
            // arriving out of order. (If we do, I think we would still want to actually fulfill
            // the validations in order.)
            assert_eq!(
                batch_offset, 0, "Batches should be processed in order. \
                 Batch {idx} is ready for validation, but the first batch is {first}.",
                idx = self.first_batch + batch_offset, first = self.first_batch,
            );
            tracing::info!("batch {} is ready for validation", self.first_batch + batch_offset);
            let batch = self.batches.pop_front().unwrap();
            self.first_batch += 1;
            return Either::left((self.first_batch + batch_offset, batch));
        } else {
            return Either::right(batch.notify.clone());
        }
    }

    pub fn is_empty(&self) -> bool {
        self.batches.is_empty() // TODO: do we need to inspect the batches?
    }
}

impl<B> IntoIterator for Batcher<'_, B> {
    type Item = BatchState<B>;
    type IntoIter = <VecDeque<BatchState<B>> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.batches.into_iter()
    }
}
