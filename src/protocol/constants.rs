// Misc
pub const SEPARATOR: &str = " ";
pub const HEARTBEAT: &str = "_heartbeat_";

// Response codes ok
pub const OK: &str = "OK";
pub const CLOSE_WAIT: &str = "CLOSE_WAIT";

// Response codes faile
pub const E_INVALID: &str = "E_INVALID";
pub const E_BAD_PROTOCOL: &str = "E_BAD_PROTOCOL";
pub const E_BAD_TOPIC: &str = "E_BAD_TOPIC";
pub const E_BAD_CHANNEL: &str = "E_BAD_CHANNEL";
pub const E_BAD_MESSAGE: &str = "E_BAD_MESSAGE";
pub const E_BAD_BODY: &str = "E_BAD_BODY";
pub const E_PUB_FAILED: &str = "E_PUB_FAILED";
pub const E_MPUB_FAILED: &str = "E_MPUB_FAILED";
pub const E_DPUB_FAILED: &str = "E_DPUB_FAILED";
pub const E_FIN_FAILED: &str = "E_FIN_FAILED";
pub const E_REQ_FAILED: &str = "E_REQ_FAILED";
pub const E_TOUCH_FAILED: &str = "E_TOUCH_FAILED";
pub const E_AUTH_FAILED: &str = "E_AUTH_FAILED";
pub const E_ANAUTHORIZED: &str = "E_ANAUTHORIZED";

// Protocol RPC commands as found here https://nsq.io/clients/tcp_protocol_spec.html
pub const IDENTIFY: &str = "IDENTIFY";
pub const SUB: &str = "SUB";
pub const PUB: &str = "PUB";
pub const MPUB: &str = "MPUB";
pub const DPUB: &str = "DPUB";
pub const RDY: &str = "RDY";
pub const FIN: &str = "FIN";
pub const REQ: &str = "REQ";
pub const TOUCH: &str = "TOUCH";
pub const CLS: &str = "CLS";
pub const NOP: &str = "NOP";
pub const AUTH: &str = "AUTH";

pub const FRAME_SIZE_HEADER_SIZE: usize = 4;
pub const FRAME_TYPE_HEADER_SIZE: usize = 4;

pub const LINE_BUFFER_PREALLOCATE_SIZE: usize = 64;
