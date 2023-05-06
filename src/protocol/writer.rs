use anyhow::Result;
use tokio::io::{AsyncWrite, AsyncWriteExt};

pub enum FrameType {
    Response = 0,
    Error = 1,
    Message = 2,
}

// [x][x][x][x][x][x][x][x][x][x][x][x]...
// |  (int32) ||  (int32) || (binary)
// |  4-byte  ||  4-byte  || N-byte
// ------------------------------------...
//     size     frame type     data
#[inline]
async fn compose_and_write_frame<W, P>(
    writer: &mut W,
    frame_type: FrameType,
    payload: P,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
    P: AsRef<[u8]>,
{
    let payload = payload.as_ref();
    writer.write_u32((4 + payload.len()) as u32).await?;
    writer.write_u32(frame_type as u32).await?;
    writer.write_all(payload).await?;
    Ok(())
}

pub async fn write_error_frame<W, M>(writer: &mut W, error_binary: M) -> Result<()>
where
    M: AsRef<[u8]>,
    W: AsyncWrite + Unpin,
{
    compose_and_write_frame(writer, FrameType::Error, error_binary).await
}

pub async fn write_response_frame<W, M>(writer: &mut W, response_binary: M) -> Result<()>
where
    M: AsRef<[u8]>,
    W: AsyncWrite + Unpin,
{
    compose_and_write_frame(writer, FrameType::Response, response_binary).await
}

pub async fn write_message_frame<W>(
    writer: &mut W,
    message: crate::message::NSQMessage,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let payload_len = 8 + 2 + 16 + message.body.len();
    writer.write_u32((4 + payload_len) as u32).await?;
    writer.write_u32(FrameType::Message as u32).await?;
    writer.write_u64(message.timestamp).await?;
    writer.write_u16(message.attempts).await?;
    writer.write_all(message.id.as_ref()).await?;
    writer.write_all(message.body.as_ref()).await?;
    Ok(())
}
