package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

func toErrString(err error) string {
	if err != nil {
		return err.Error()
	}

	return ""
}

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	resp := &kvrpcpb.RawGetResponse{
		NotFound: true,
	}
	// Your Code Here (1).
	r, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = toErrString(err)
		return resp, nil
	}

	defer r.Close()

	res, err := r.GetCF(req.Cf, req.Key)
	if err != nil {
		resp.Error = toErrString(err)
		return resp, nil
	}

	resp.Value = res
	resp.NotFound = res == nil

	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	err := server.storage.Write(req.Context, []storage.Modify{
		{
			Data: storage.Put{
				Key:   req.Key,
				Value: req.Value,
				Cf:    req.Cf,
			},
		},
	})

	if err != nil {
		return nil, err
	}

	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	err := server.storage.Write(req.Context, []storage.Modify{
		{
			Data: storage.Delete{
				Key: req.Key,
				Cf:  req.Cf,
			},
		},
	})

	return &kvrpcpb.RawDeleteResponse{
		Error: toErrString(err),
	}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	r, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}

	defer r.Close()

	iter := r.IterCF(req.Cf)
	defer iter.Close()
	iter.Seek(req.StartKey)
	res := make([]*kvrpcpb.KvPair, 0, req.Limit)
	i := req.Limit
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		item := iter.Item()
		val, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}

		res = append(res, &kvrpcpb.KvPair{
			Key:   item.KeyCopy(nil),
			Value: val,
		})
		i--
		if i == 0 {
			break
		}
	}

	return &kvrpcpb.RawScanResponse{
		Kvs: res,
	}, nil
}
