package app

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/axelarnetwork/axelar-core/x/nexus/exported"

	"bharvest.io/axelmon/client/api"
	"bharvest.io/axelmon/client/grpc"
	"bharvest.io/axelmon/metrics"
	"bharvest.io/axelmon/server"
	"github.com/prometheus/client_golang/prometheus"
)

func (c *Config) checkEVMVotes(ctx context.Context) error {
	client := grpc.New(c.General.GRPC)
	err := client.Connect(ctx, c.General.GRPCSecureConnection)
	defer client.Terminate(ctx)
	if err != nil {
		return err
	}

	chains, err := client.GetChains(ctx)
	if err != nil {
		return err
	}

	return c.checkPollingVotes(ctx, api.EVM_POLLING_TYPE, chains)
}

func (c *Config) checkVMVotes(ctx context.Context) error {
	chains, err := api.C.GetVerifierSupportedChains(c.Wallet.Proxy.PrintAcc())
	if err != nil {
		return err
	}
	return c.checkPollingVotes(ctx, api.VM_POLLING_TYPE, chains)
}

func (c *Config) checkPollingVotes(ctx context.Context, pollingType api.PollingType, chains []exported.ChainName) error {
	if c.PollingVote.LastProcessedVotes == nil {
		c.PollingVote.LastProcessedVotes = make(map[string]map[string]byte)
	}

	result := make(map[string]server.VotesInfo)
	for _, chain := range chains {
		// If chain is included in except chains
		// then don't monitor that chain's EVM votes.
		if c.General.ExceptChains[strings.ToLower(chain.String())] {
			continue
		}

		votesInfo := server.VotesInfo{}

		if c.PollingVote.CheckPeriodDays == 0 {
			c.PollingVote.CheckPeriodDays = 10
		}
		resp, err := api.C.GetPollingVotes(chain.String(), c.PollingVote.CheckN, c.Wallet.Proxy.PrintAcc(), pollingType,
			time.Duration(c.PollingVote.CheckPeriodDays)*time.Hour*24)
		if err != nil {
			return err
		}

		votesInfo.Missed = fmt.Sprintf("%d / %d", resp.MissCnt, int(resp.TotalVotes))

		if c.PollingVote.LastProcessedVotes[chain.String()] == nil {
			c.PollingVote.LastProcessedVotes[chain.String()] = make(map[string]byte)
		}

		// get only the new votes
		var newVotesMissed int
		var newVotesSuccess int

		for _, voteInfo := range resp.VoteInfos {
			if _, exists := c.PollingVote.LastProcessedVotes[chain.String()][voteInfo.PollID]; !exists {
				if voteInfo.IsSkipped {
					continue
				}
				if voteInfo.IsLate || voteInfo.Vote != 1 {
					newVotesMissed++
				} else {
					newVotesSuccess++
				}
				c.PollingVote.LastProcessedVotes[chain.String()][voteInfo.PollID] = voteInfo.Vote
			}
		}

		// remove polls that are no more in the response of the api
		var keysToDelete []string
		for key := range c.PollingVote.LastProcessedVotes[chain.String()] {
			var found bool
			for _, voteInfo := range resp.VoteInfos {
				if voteInfo.PollID == key {
					found = true
					break
				}
			}
			if !found {
				keysToDelete = append(keysToDelete, key)
			}
		}
		for _, key := range keysToDelete {
			delete(c.PollingVote.LastProcessedVotes[chain.String()], key)
		}
		
		// use the metrics based on the pollingType
		if pollingType == api.EVM_POLLING_TYPE {
			metrics.EVMVotesCounter.With(prometheus.Labels{"network_name": chain.String(), "status": "missed"}).Add(float64(newVotesMissed))
			metrics.EVMVotesCounter.With(prometheus.Labels{"network_name": chain.String(), "status": "success"}).Add(float64(newVotesSuccess))
		} else {
			metrics.AmplifierPollsCounter.With(prometheus.Labels{"network_name": chain.String(), "status": "missed"}).Add(float64(newVotesMissed))
			metrics.AmplifierPollsCounter.With(prometheus.Labels{"network_name": chain.String(), "status": "success"}).Add(float64(newVotesSuccess))
		}

		if (float64(resp.MissCnt)/resp.TotalVotes)*100 > float64(c.PollingVote.MissPercentage) {
			votesInfo.Status = false

			msg := fmt.Sprintf("%s status(%s)", pollingType, chain)
			c.alert(msg, []string{}, false, false)
		} else {
			votesInfo.Status = true

			msg := fmt.Sprintf("%s status(%s)", pollingType, chain)
			c.alert(msg, []string{}, true, false)
		}

		result[chain.String()] = votesInfo
	}
	server.GlobalState.EVMVotes.Chain = result

	return nil
}
