package keeper

import (
	"fmt"

	sdk "github.com/line/lbm-sdk/types"
	sdkerrors "github.com/line/lbm-sdk/types/errors"

	"github.com/line/lbm-sdk/x/foundation"
)

// ensureMsgAuthz checks that if a message requires signers that all of them are equal to the given account address of the authority.
func ensureMsgAuthz(msgs []sdk.Msg, authority sdk.AccAddress) error {
	for _, msg := range msgs {
		// In practice, GetSigners() should return a non-empty array without
		// duplicates, so the code below is equivalent to:
		// `msgs[i].GetSigners()[0] == authority`
		// but we prefer to loop through all GetSigners just to be sure.
		for _, signer := range msg.GetSigners() {
			if !authority.Equals(signer) {
				return sdkerrors.ErrUnauthorized.Wrapf("msg does not have foundation authorization; expected %s, got %s", authority, signer)
			}
		}
	}
	return nil
}

func (k Keeper) Exec(ctx sdk.Context, proposalID uint64) error {
	proposal, err := k.GetProposal(ctx, proposalID)
	if err != nil {
		return err
	}

	if proposal.Status != foundation.PROPOSAL_STATUS_SUBMITTED &&
		proposal.Status != foundation.PROPOSAL_STATUS_ACCEPTED { // 언제 accepted로 되나? accepted로 된건 이미 exec된거 아닌가? -> 아... try로 들어가서 자동적으로 tally를 한 proposal은 들어와도 exec을 수행 안했을 수 있겠군!
		return sdkerrors.ErrInvalidRequest.Wrapf("not possible with proposal status: %s", proposal.Status)
	}

	if proposal.Status == foundation.PROPOSAL_STATUS_SUBMITTED {
		if err := k.doTallyAndUpdate(ctx, proposal); err != nil {
			return err
		}
	}

	// Execute proposal payload.
	var logs string
	if proposal.Status == foundation.PROPOSAL_STATUS_ACCEPTED &&
		proposal.ExecutorResult != foundation.PROPOSAL_EXECUTOR_RESULT_SUCCESS { // 애초에 executor result가 success라면 여기 들어올 필요조차 없는게 아닌가?
		logger := ctx.Logger().With("module", fmt.Sprintf("x/%s", foundation.ModuleName))
		// Caching context so that we don't update the store in case of failure.
		ctx, flush := ctx.CacheContext()

		if _, err = k.doExecuteMsgs(ctx, *proposal); err != nil {
			proposal.ExecutorResult = foundation.PROPOSAL_EXECUTOR_RESULT_FAILURE
			logs = fmt.Sprintf("proposal execution failed on proposal %d, because of error %s", proposalID, err.Error())
			logger.Info("proposal execution failed", "cause", err, "proposalID", proposal.Id)
		} else {
			proposal.ExecutorResult = foundation.PROPOSAL_EXECUTOR_RESULT_SUCCESS
			flush()
		}
	}

	// If proposal has successfully run, delete it from state.
	if proposal.ExecutorResult == foundation.PROPOSAL_EXECUTOR_RESULT_SUCCESS {
		k.pruneProposal(ctx, *proposal)
	} else {
		k.setProposal(ctx, *proposal) // 이건 왜 해주는건가? prune안했으면 그대로 store에 남아있는거 아닌가?
	}

	if err := ctx.EventManager().EmitTypedEvent(&foundation.EventExec{
		ProposalId: proposal.Id,
		Logs:       logs,
		Result:     proposal.ExecutorResult,
	}); err != nil {
		panic(err)
	}

	return nil
}

// doExecuteMsgs routes the messages to the registered handlers.
func (k Keeper) doExecuteMsgs(ctx sdk.Context, proposal foundation.Proposal) ([]sdk.Result, error) {
	msgs := proposal.GetMsgs()
	results := make([]sdk.Result, len(msgs))

	authority := sdk.MustAccAddressFromBech32(k.GetAuthority())
	if err := ensureMsgAuthz(msgs, authority); err != nil {
		return nil, err
	}

	for i, msg := range msgs {
		handler := k.router.Handler(msg)
		if handler == nil {
			return nil, sdkerrors.ErrUnknownRequest.Wrapf("no message handler found for %q", sdk.MsgTypeURL(msg))
		}
		r, err := handler(ctx, msg)
		if err != nil {
			return nil, sdkerrors.Wrapf(err, "message %q at position %d", msg, i)
		}
		if r != nil {
			results[i] = *r
		}
	}
	return results, nil
}
