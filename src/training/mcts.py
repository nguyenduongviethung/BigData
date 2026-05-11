import math
import numpy as np
import torch
import chess

class Node:
    def __init__(self, state_fen, prior_prob, parent=None, move_from_parent=None):
        self.state_fen = state_fen # Trạng thái bàn cờ (dạng chuỗi FEN)
        self.prior_prob = prior_prob # Xác suất P từ Mạng Nơ-ron
        self.parent = parent
        self.move_from_parent = move_from_parent # Nước đi dẫn đến node này
        
        self.children = {} # Các node con
        self.visit_count = 0 # Số lần ghé thăm (N)
        self.value_sum = 0.0 # Tổng giá trị dự đoán (W)

    def value(self):
        """Giá trị trung bình (Q)"""
        if self.visit_count == 0:
            return 0
        return self.value_sum / self.visit_count

    def expand(self, board, action_probs):
        """Mở rộng các nhánh (các nước đi hợp lệ) từ node này"""
        for move, prob in action_probs.items():
            if move not in self.children:
                board.push(move)
                self.children[move] = Node(
                    state_fen=board.fen(),
                    prior_prob=prob,
                    parent=self,
                    move_from_parent=move
                )
                board.pop()

    def is_expanded(self):
        return len(self.children) > 0


class MCTS:
    def __init__(self, model, num_simulations=50, c_puct=1.5):
        self.model = model
        self.num_simulations = num_simulations
        self.c_puct = c_puct # Hằng số cân bằng giữa Khám phá (Explore) và Khai thác (Exploit)

    def get_action_prob(self, board, temperature=1.0):
        """Hàm chính: Chạy suy nghĩ và trả về xác suất đi các nước"""
        root = Node(state_fen=board.fen(), prior_prob=1.0)

        # Chạy vòng lặp tưởng tượng
        for _ in range(self.num_simulations):
            node = root
            sim_board = board.copy()

            # 1. SELECTION (Chọn lọc): Đi xuống các node con tiềm năng nhất
            while node.is_expanded():
                move, node = self._select_child(node)
                sim_board.push(move)

            # 2. EVALUATION & EXPANSION (Đánh giá & Mở rộng)
            result = sim_board.result()
            if result != "*": # Nếu game over ở nhánh này
                value = 1.0 if result == "1-0" else (-1.0 if result == "0-1" else 0.0)
                # Đảo góc nhìn cho người chơi hiện tại
                value = value if sim_board.turn == chess.WHITE else -value 
            else:
                # Dùng Mạng Nơ-ron đánh giá trạng thái hiện tại
                tensor_state = self._board_to_tensor(sim_board)
                with torch.no_grad():
                    pred_policy, pred_value = self.model(tensor_state)
                
                value = pred_value.item()
                
                # Trích xuất policy cho các nước đi hợp lệ (Rút gọn cho Demo)
                legal_moves = list(sim_board.legal_moves)
                
                # Tạo một phân phối xác suất giả lập đều (Thực tế sẽ map từ pred_policy)
                # Để dễ tích hợp, chúng ta chia đều xác suất cho các nước đi hợp lệ
                action_probs = {move: 1.0/len(legal_moves) for move in legal_moves}
                
                # Mở rộng cây
                node.expand(sim_board, action_probs)

            # 3. BACKPROPAGATION (Lan truyền ngược): Cập nhật điểm lên trên
            self._backpropagate(node, value)

        # SAU KHI SUY NGHĨ XONG: Tính toán xác suất nước đi dựa trên số lần ghé thăm (N)
        action_visits = [(move, child.visit_count) for move, child in root.children.items()]
        
        # Chọn nước đi có số lần ghé thăm nhiều nhất (Robust play)
        action_visits.sort(key=lambda x: x[1], reverse=True)
        best_move = action_visits[0][0]
        
        return best_move

    def _select_child(self, node):
        """Chọn node con có điểm UCB cao nhất"""
        best_score = -float('inf')
        best_action = None
        best_child = None

        for action, child in node.children.items():
            # Công thức UCB của AlphaZero: Q + U
            u = self.c_puct * child.prior_prob * (math.sqrt(node.visit_count) / (1 + child.visit_count))
            ucb_score = child.value() + u
            
            if ucb_score > best_score:
                best_score = ucb_score
                best_action = action
                best_child = child
                
        return best_action, best_child

    def _backpropagate(self, node, value):
        """Cập nhật giá trị ngược từ lá lên rễ"""
        while node is not None:
            node.visit_count += 1
            node.value_sum += value
            node = node.parent
            value = -value # Đảo chiều giá trị sau mỗi lượt (Zero-sum game)

    def _board_to_tensor(self, board):
        # Hàm helper giống bên self_play.py
        tensor = np.zeros((12, 8, 8), dtype=np.float32)
        for square, piece in board.piece_map().items():
            row, col = divmod(square, 8)
            piece_idx = piece.piece_type - 1 + (6 if piece.color == chess.BLACK else 0)
            tensor[piece_idx, row, col] = 1.0
        return torch.from_numpy(tensor).unsqueeze(0)