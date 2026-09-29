//! Carrying a [`Step`] out: the one place that decides in which order a
//! step's grant, messages and authority calls reach the world outside the
//! node. Every driver (the network driver, the single-process runtime, the
//! simulator) goes through [`carry_out`], so none of them re-derives that
//! order.

use std::collections::VecDeque;

use crate::protocol::ids::{IdGenerator, WorkerId};
use crate::protocol::messages::ElectionMessage;
use crate::scheduler::Scheduler;
use crate::time::{Clock, Instant};

use super::{AuthorityCall, AuthorityReply, Input, Output, Step, WorkerNode, apply_to_scheduler};

/// Where a step's messages go: a real transport, a simulated network, or
/// nowhere for a node with no peers ([`DropMessages`]). Delivery need not be
/// immediate or reliable (see [`Output::Send`] and [`Output::Publish`]).
pub trait MessageSink {
    /// Sends `message` to the worker `to`.
    fn send(&mut self, to: WorkerId, message: ElectionMessage);
    /// Publishes `message` to every worker subscribed to the node's shard.
    fn publish(&mut self, message: ElectionMessage);
}

/// Who performs a step's authority calls (see [`Output::Authority`]).
pub trait AuthorityPerformer {
    /// Performs `call`, or starts it. `Some(reply)` is handed back to the
    /// node at once, by [`carry_out`]; `None` means the reply, if any, comes
    /// later, and the driver hands it to the node as [`Input::Authority`]
    /// itself.
    fn perform(&mut self, call: AuthorityCall) -> Option<AuthorityReply>;
}

/// A [`MessageSink`] that drops every message: for a node with no peers.
pub struct DropMessages;

impl MessageSink for DropMessages {
    fn send(&mut self, _: WorkerId, _: ElectionMessage) {}

    fn publish(&mut self, _: ElectionMessage) {}
}

/// An [`AuthorityPerformer`] with no authority to reach: it answers every
/// call at once as failed with `Unavailable` (see
/// [`AuthorityCall::unavailable`]), so a node never waits on an authority
/// that does not exist.
pub struct NoAuthority;

impl AuthorityPerformer for NoAuthority {
    fn perform(&mut self, call: AuthorityCall) -> Option<AuthorityReply> {
        Some(call.unavailable())
    }
}

/// Carries out `first`, a step `node` has just taken, and every step it
/// leads to. For each step, in order:
///
/// 1. its leadership grant and lost workers go to `scheduler`, so a grant
///    the step withdraws is gone before anything the step sends can let
///    another leader act;
/// 2. `observe` is called with the node, the scheduler, the input that
///    caused the step (`None` for `first`) and the step itself;
/// 3. its messages are sent and published through `sink`;
/// 4. its authority calls go to `performer`, and each reply it gives at once
///    is handed to `node` as [`Input::Authority`], in order, each resulting
///    step carried out the same way before this returns.
///
/// State changes, the abort deadline and alerts need no action here: a
/// driver that acts on them reads them in `observe`. Returns the deadline
/// the last step reported (see [`Step::next_deadline`]).
///
/// `scheduler` must read the clock the node reads: a grant's lease ends at
/// an instant of the node's clock, and the scheduler compares it with its
/// own.
pub fn carry_out<C, I, S, P>(
    node: &mut WorkerNode<C>,
    first: Step,
    scheduler: &mut Scheduler<C, I>,
    sink: &mut S,
    performer: &mut P,
    mut observe: impl FnMut(&WorkerNode<C>, &Scheduler<C, I>, Option<&Input>, &Step),
) -> Option<Instant>
where
    C: Clock,
    I: IdGenerator,
    S: MessageSink,
    P: AuthorityPerformer,
{
    let mut replies = VecDeque::new();
    let mut input = None;
    let mut step = first;
    loop {
        apply_to_scheduler(&step.outputs, scheduler);
        observe(node, scheduler, input.as_ref(), &step);
        for output in &step.outputs {
            match output {
                Output::Send { to, message } => sink.send(to.clone(), message.clone()),
                Output::Publish { message } => sink.publish(message.clone()),
                _ => {}
            }
        }
        for output in &step.outputs {
            if let Output::Authority(call) = output
                && let Some(reply) = performer.perform(*call)
            {
                replies.push_back(Input::Authority(reply));
            }
        }
        let Some(next) = replies.pop_front() else {
            return step.next_deadline;
        };
        step = node.step(next.clone());
        input = Some(next);
    }
}
