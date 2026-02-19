use s2_sdk::types::{
    AppendInput, AppendRecord, AppendRecordBatch, ReadFrom, ReadInput, ReadStart, S2Error,
};
use s2_sdk::{S2, S2Basin, S2Stream};

use crate::error::{OrchestratorError, Result};
use crate::types::*;

const EVENTS_STREAM: &str = "events";

#[derive(Clone)]
pub struct OrchestratorStreams {
    pub basin: S2Basin,
}

impl OrchestratorStreams {
    pub fn new(basin: S2Basin) -> Self {
        Self { basin }
    }

    pub fn stream(&self, name: &str) -> Result<S2Stream> {
        Ok(self.basin.stream(
            name.parse()
                .map_err(|e| OrchestratorError::S2Init(format!("Invalid stream name '{name}': {e:?}")))?,
        ))
    }

    async fn read_all_records<T, F>(&self, stream: S2Stream, mut parse: F) -> Result<Vec<T>>
    where
        F: FnMut(&[u8]) -> Result<T>,
    {
        let mut items = Vec::new();
        let mut start_seq = 0u64;

        loop {
            let input = ReadInput::new()
                .with_start(ReadStart::new().with_from(ReadFrom::SeqNum(start_seq)));

            match stream.read(input).await {
                Ok(batch) => {
                    if batch.records.is_empty() {
                        break;
                    }
                    for record in batch.records {
                        start_seq = record.seq_num + 1;
                        items.push(parse(&record.body)?);
                    }
                }
                Err(S2Error::ReadUnwritten(_)) => break,
                Err(e) => return Err(e.into()),
            }
        }

        Ok(items)
    }

    pub async fn emit_event(&self, event: &Event) -> Result<()> {
        let stream = self.stream(EVENTS_STREAM)?;
        let json = serde_json::to_vec(event)?;
        let record = AppendRecord::new(json).map_err(|e| OrchestratorError::S2Init(e.to_string()))?;
        let batch = AppendRecordBatch::try_from_iter([record])
            .map_err(|e| OrchestratorError::S2Init(e.to_string()))?;
        stream.append(AppendInput::new(batch)).await?;
        Ok(())
    }

    pub async fn publish_strategy(
        &self,
        swarm_id: &RunId,
        strategy: &crate::swarm::Strategy,
    ) -> Result<()> {
        let stream_name = format!("swarm/{}/plan", swarm_id.0);
        let stream = self.stream(&stream_name)?;
        let json = serde_json::to_vec(strategy)?;
        let record = AppendRecord::new(json).map_err(|e| OrchestratorError::S2Init(e.to_string()))?;
        let batch = AppendRecordBatch::try_from_iter([record])
            .map_err(|e| OrchestratorError::S2Init(e.to_string()))?;
        stream.append(AppendInput::new(batch)).await?;
        Ok(())
    }

    pub async fn read_strategy(
        &self,
        swarm_id: &RunId,
    ) -> Result<Option<crate::swarm::Strategy>> {
        let stream_name = format!("swarm/{}/plan", swarm_id.0);
        let stream = self.stream(&stream_name)?;
        let mut strategies =
            self.read_all_records(stream, |body| Ok(serde_json::from_slice(body)?))
                .await?;
        Ok(strategies.pop())
    }

    pub async fn send_message(
        &self,
        swarm_id: &RunId,
        msg: &SwarmMessage,
    ) -> Result<()> {
        let stream_name = format!("swarm/{}/messages", swarm_id.0);
        let stream = self.stream(&stream_name)?;
        let json = serde_json::to_vec(msg)?;
        let record = AppendRecord::new(json).map_err(|e| OrchestratorError::S2Init(e.to_string()))?;
        let batch = AppendRecordBatch::try_from_iter([record])
            .map_err(|e| OrchestratorError::S2Init(e.to_string()))?;
        stream.append(AppendInput::new(batch)).await?;
        Ok(())
    }

}

pub fn connect(config: &crate::config::Config, basin_override: Option<&str>) -> Result<OrchestratorStreams> {
    let token = config.s2_access_token()?;
    let basin_name = config.basin_name(basin_override)?;

    let mut s2_config = s2_sdk::types::S2Config::new(token);

    if let (Some(account_ep), Some(basin_ep)) = (
        &config.s2.account_endpoint,
        &config.s2.basin_endpoint,
    ) {
        let endpoints = s2_sdk::types::S2Endpoints::new(
            s2_sdk::types::AccountEndpoint::new(account_ep)
                .map_err(|e| OrchestratorError::S2Init(e.to_string()))?,
            s2_sdk::types::BasinEndpoint::new(basin_ep)
                .map_err(|e| OrchestratorError::S2Init(e.to_string()))?,
        )
        .map_err(|e| OrchestratorError::S2Init(e.to_string()))?;
        s2_config = s2_config.with_endpoints(endpoints);
    }

    let s2 = S2::new(s2_config).map_err(|e| OrchestratorError::S2Init(e.to_string()))?;
    let basin = s2.basin(
        basin_name
            .parse()
            .map_err(|e| OrchestratorError::S2Init(format!("{e:?}")))?,
    );

    Ok(OrchestratorStreams::new(basin))
}
